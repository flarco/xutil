import os, sys, argparse
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


def alias_cli():
  "Install alias"
  from xutil.helpers import get_home_path, get_dir_path, get_script_path
  from xutil.diskio import read_file, write_file
  from shutil import copyfile
  ans = input("Install 'alias.sh' in home directory (Y to proceed)? ")
  if ans.lower() != 'y':
    return

  src_path = get_dir_path() + '/alias.sh'
  dst_path = get_home_path() + '/.alias.sh'
  bash_profile_path = get_home_path() + '/.bash_profile'

  # log('src_path -> ' + src_path)
  # log('dst_path -> ' + dst_path)
  copyfile(src_path, dst_path)

  bash_prof_text = read_file(bash_profile_path)

  if not dst_path in bash_prof_text:
    bash_prof_text = '{}\n\n. {}'.format(bash_prof_text, dst_path)
    write_file(bash_profile_path, bash_prof_text)
    log('+Updated ' + bash_profile_path)


def exec_sql():
  from xutil.database.etl import SqlCmdParser
  SqlCmdParser()


def exec_etl():
  from xutil.database.etl import EtlCmdParser
  EtlCmdParser()