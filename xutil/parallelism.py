# Parallelism Lib
'''
Threads
Multiprocessing
'''
import os, socket, time
from .helpers import (log, slog, elog, get_exception_message, now,
                      ndelta_seconds, get_home_path, cleanup_pid, register_pid,
                      get_pid_path)
from .diskio import (write_file, read_file)
from functools import wraps

from collections import (
  OrderedDict,
  namedtuple,
)
from threading import (
  Thread,
  Lock,
)
from multiprocessing import Process
from multiprocessing import (Queue)

all_threads = OrderedDict()
all_processes = OrderedDict()
th_lock = Lock()


def pipe_post(pipe, obj):
  "Send object through multiprocessing Pipe and wait for response"


class Pipe:
  def __init__(self):
    import multiprocessing
    self.lock = multiprocessing.Lock()
    self.parent, self.child = multiprocessing.Pipe()

  def emit_to_parent(self, obj):
    "Send object through multiprocessing Pipe and wait for response"
    self.child.send(obj)
    return self.child.recv()

  def emit_to_child(self, obj):
    "Send object through multiprocessing Pipe and wait for response"
    self.parent.send(obj)
    return self.parent.recv()

  def send_to_parent(self, obj):
    "Send object through multiprocessing Pipe"
    self.child.send(obj)

  def send_to_child(self, obj):
    "Send object through multiprocessing Pipe"
    self.parent.send(obj)

  def recv_from_parent(self, timeout=-1):
    "Receive object through multiprocessing Pipe. timeout in sec. timeout=-1 means wait forever."
    s_t = now()
    if timeout == -1:
      return self.child.recv()
    elif timeout == 0:
      if self.child.poll():
        return self.child.recv()
    else:
      while 1:
        if self.child.poll():
          return self.child.recv()
        elif ndelta_seconds(s_t) <= timeout:
          break
      return None

  def recv_from_child(self, timeout=-1):
    "Receive object through multiprocessing Pipe. timeout in sec. timeout=-1 means wait forever."
    s_t = now()
    if timeout == -1:
      return self.parent.recv()
    elif timeout == 0:
      if self.parent.poll():
        return self.parent.recv()
    else:
      while 1:
        if self.parent.poll():
          return self.parent.recv()
        elif ndelta_seconds(s_t) <= timeout:
          break
      return None


class Worker:
  kwargs_reserved = ['worker']

  def __init__(self,
               name,
               type,
               fn,
               log=log,
               args=[],
               kwargs={},
               start=False,
               kill_if_running=False,
               pid_folder=None):
    self.hostname = socket.gethostname()
    self.name = name
    self.type = type
    self.pipe = Pipe()  # 2-way
    self.child_q = Queue()
    self.parent_q = Queue()
    self.lock = Lock()
    self.fn = fn
    self.log = log
    self.args = args
    self.kwargs = kwargs
    self.kill_if_running = kill_if_running
    self.started = False
    pid_folder = pid_folder if pid_folder else get_home_path()
    self.pid_file = get_pid_path(name, pid_folder)
    self.status = None

    for key in self.kwargs_reserved:
      if key in kwargs:
        log('kwargs = {}'.format(kwargs))
        raise Exception(
          'Cannot use reserved word "{}" in kwargs for worker {}'.format(
            key, name))

    self.kwargs['worker'] = self
    self.process = Process(target=fn, args=self.args, kwargs=self.kwargs)
    self.process.name = name

    if start:
      self.start()

  def start(self):
    # kill existing PID
    cleanup_pid(self.pid_file, kill_if_running=self.kill_if_running)

    self.process.start()
    self.started = now()
    self.pid = self.process.pid
    register_pid(self.pid_file, self.pid, clean_atexit=True)

  def stop(self):
    self.process.terminate()

    # delete pid file
    cleanup_pid(self.pid_file, kill_if_running=True)

  def put_parent_q(self, obj):
    self.parent_q.put(obj)

  def put_child_q(self, obj):
    self.child_q.put(obj)

  def get_parent_q(self):
    if not self.parent_q.empty():
      return self.parent_q.get()
    else:
      return None

  def get_child_q(self):
    if not self.child_q.empty():
      return self.child_q.get()
    else:
      return None


def kill_processes(name):
  import sys, psutil, signal

  signal_val = int(sys.argv[2]) if len(sys.argv) == 3 else signal.SIGTERM
  signal_val = 9

  user = os.getenv('USER')
  pid = os.getpid()

  killed = []
  for proc in psutil.process_iter():
    if proc.username() != user: continue
    cmdline = ' '.join(proc.cmdline()).replace('\n', ' ')
    if not name in cmdline: continue
    if proc.pid == pid: continue
    print("Killing {} : {}".format(proc.pid, cmdline))
    killed.append(proc.pid)
    os.kill(proc.pid, signal_val)  # else, kill


def interrupt(proc_name=None, th_name=None):
  global all_threads, all_processes, th_lock

  def kill_proc(p):
    log('Killing process PID {} -> {}'.format(p.pid, p.name))
    p.terminate()
    os.system('kill -9 {}'.format(p.pid))

  def kill_thread(t):
    log('Killing thread -> {}'.format(t.name))
    t.cancel()

  if proc_name:
    kill_proc(all_processes[proc_name])
    return

  if th_name:
    kill_thread(all_threads[th_name])
    return

  for proc in all_processes.values():
    with th_lock:
      kill_proc(proc)

  for th in all_threads.values():
    with th_lock:
      kill_thread(th)


def get_running_threads():
  global all_threads
  threads_running = [th.name for th in all_threads.values() if th.isAlive()]
  return threads_running


def get_running_processes():
  global all_processes
  proc_running = [
    proc.name for proc in all_processes.values() if proc.is_alive()
  ]
  return proc_running


def join_threads():
  "Join all running threads: wait until done"
  global all_threads
  running = [1, 1]
  while running:
    time.sleep(1)
    with th_lock:
      running = [th for th in all_threads.values() if th.isAlive()]
    # th.join()


def run_async(func):
  """
    run_async(func)
      function decorator, intended to make "func" run in a separate
      thread (asynchronously).
      Returns the created Thread object
      E.g.:
      @run_async
      def task1():
        do_something
      @run_async
      def task2():
        do_something_too
      t1 = task1()
      t2 = task2()
      ...
      t1.join()
      t2.join()
  """

  @wraps(func)
  def async_func(*args, **kwargs):
    global all_threads, th_lock

    f_name = '_'.join([func.__name__, str(args), str(kwargs)])
    func_hl = None

    if f_name in all_threads:
      if all_threads[f_name].isAlive():
        func_hl = all_threads[f_name]

    if not func_hl:
      func_hl = Thread(name=f_name, target=func, args=args, kwargs=kwargs)
      func_hl.start()
      log('Thread Started -> {}'.format(func_hl.name))

    with th_lock:
      all_threads[f_name] = func_hl

    return func_hl

  return async_func


def run_async_process(func):
  """
    run_async_process(func)
      function decorator, intended to make "func" run in a separate
      thread (asynchronously).
      Returns the created Thread object
      E.g.:
      @run_async
      def task1():
        do_something
      @run_async
      def task2():
        do_something_too
      t1 = task1()
      t2 = task2()
      ...
      t1.join()
      t2.join()
  """
  from multiprocessing import Process

  @wraps(func)
  def async_func(*args, **kwargs):
    global all_processes, th_lock

    f_name = '_'.join([func.__name__, str(args), str(kwargs)])
    func_hl = None

    if f_name in all_processes:
      if all_processes[f_name].is_alive():
        func_hl = all_processes[f_name]

    if not func_hl:
      func_hl = Process(name=f_name, target=func, args=args, kwargs=kwargs)
      func_hl.start()
      log('Process Started PID {} -> {}'.format(func_hl.pid, func_hl.name))

    with th_lock:
      all_processes[f_name] = func_hl

    return func_hl

  return async_func