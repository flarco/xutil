import os, sys
from xutil import log


def pykill(*args_):
  "Kill processes based on name"

  import psutil

  args = sys.argv[1:] or args_
  if not args:
    log('~Need one argument to kill processes!')
    return

  kw = args[0]
  user = os.getenv('USER')
  skip_list = set(['login'])
  this_pid = os.getpid()

  procs_matched = []
  for proc in psutil.process_iter():
    pid = proc.pid
    name = proc.name()

    if not kw.strip(
    ) or user != proc.username() or name in skip_list or pid == this_pid:
      continue
    cmdline = ' '.join(proc.cmdline())

    try:
      if kw.lower() in cmdline.lower():
        proc.terminate()
        log('+Killed {} : {}'.format(pid, cmdline))
        procs_matched.append(proc)
    except Exception as E:
      log('-Could not kill {} : {}'.format(pid, name))
      log(E)

    if len(procs_matched) == 0:
      log('-Did not match any process name.')


def exec_sql():
  args = sys.argv[1:]
  if not args:
    log('~Need one argument to run SQL!')
    return
  else:
    log('+Ran SQL!')
