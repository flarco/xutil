# Parallelism Lib
'''
Threads
Multiprocessing
'''
from .helpers import (log, slog, elog, get_exception_message)

from collections import (
  OrderedDict,
  namedtuple,
)
from threading import (
  Thread,
  Lock,
)
all_threads = OrderedDict()
all_processes = OrderedDict()
th_lock = Lock()


def kill_processes(name):
  import os, sys, psutil, signal

  # signal_val = int(sys.argv[2]) if len(sys.argv) == 3 else signal.SIGTERM
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