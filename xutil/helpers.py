# General Helpers Lib
import datetime, time
import json, atexit
import os, signal
import re, traceback
import sys, socket, psutil
from collections import namedtuple, OrderedDict
from decimal import Decimal
from pathlib import Path

import dateutil
try:
  from colorama import Fore, Back, Style
  color_enabled = True
except ImportError:
  color_enabled = False

host_name = socket.gethostname()

### LAMBDAS ############################

get_rec = lambda row, headers: struct({h.lower():row[i] for i,h in enumerate(headers)})
now = lambda: datetime.datetime.now()
epoch = lambda: int(time.time())
epoch_mil = lambda: int(time.time() * 1000)
now_plus = lambda **kwargs: datetime.datetime.now() + datetime.timedelta(**kwargs)
now_minus = lambda **kwargs: datetime.datetime.now() - datetime.timedelta(**kwargs)
tdelta_seconds = lambda first, second: (first - second).total_seconds()
tdelta_minutes = lambda first, second: (first - second).total_seconds() / 60
tdelta_hours = lambda first, second: (first - second).total_seconds() / 60 / 60
tdelta_days = lambda first, second: (first - second).total_seconds() / 60 / 60 / 24
ndelta_seconds = lambda past: tdelta_seconds(now(), past)  # now delta seconds
ndelta_minutes = lambda past: tdelta_minutes(now(), past)  # now delta minutes
now_str = lambda: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
now_file_str = lambda: datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
today_str = lambda: datetime.datetime.now().strftime('%Y%m%d')
get_row_fields = lambda r: r._fields if hasattr(r, '_fields') else r.__fields__
jdumps = lambda obj: json.dumps(obj, cls=MyJSONEncoder)
jloads = lambda obj: json.loads(obj)
jtrans = lambda obj: jloads(jdumps(obj))
is_gen_func = lambda x: str(x).startswith('<generator ')
get_profile = lambda create_if_missing=False: load_profile(create_if_missing=create_if_missing)
get_kw = lambda k, deflt, kwargs: kwargs[k] if k in kwargs else deflt

get_script_path = lambda: os.path.dirname(os.path.realpath(sys.argv[0]))
get_dir_path = lambda f=__file__: os.path.dirname(f)
get_home_path = lambda: os.path.expanduser("~")
get_file_name = lambda f=__file__: os.path.splitext(os.path.basename(f))[0]
file_exists = lambda p: Path(p).exists()
kill_pid = lambda pid: os.kill(pid, signal.SIGTERM)
get_pid_path = lambda name=__file__, folder=get_home_path(): '{}/.{}.PID'.format(folder, name)
make_rec = lambda **d: namedtuple('Rec', d.keys())(**d)
set_nice = lambda n: psutil.Process(os.getpid()).nice(n)

elog = lambda text, show_time=True: log(text, color='red', show_time=show_time)
slog = lambda text, show_time=True: log(text, color='green', show_time=show_time)

## LOGGING ############################

try:
  # https://pypi.org/project/coloredlogs/
  # https://coloredlogs.readthedocs.io/en/latest/api.html#changing-the-log-format
  os.environ[
    'COLOREDLOGS_LOG_FORMAT'] = '[%(hostname)s] %(asctime)s %(levelname)s | %(message)s'
  os.environ['COLOREDLOGS_LOG_FORMAT'] = '%(asctime)s -- %(message)s'
  os.environ['COLOREDLOGS_LOG_FORMAT'] = '%(asctime) s%(sep) s%(message)s'

  import coloredlogs, verboselogs
  logger = verboselogs.VerboseLogger(__name__)

  coloredlogs.DEFAULT_FIELD_STYLES = {
    'asctime': {
      'color': 'white'
    },
    'sep': {
      'color': 'magenta'
    },
    'hostname': {
      'color': 'magenta'
    },
    'levelname': {
      'color': 'black',
      'bold': True
    },
    'name': {
      'color': 'blue'
    },
    'programname': {
      'color': 'cyan'
    }
  }

  coloredlogs.DEFAULT_LEVEL_STYLES = {
    'critical': {
      'color': 'red',
      'bold': True
    },
    'debug': {
      'color': 'green'
    },
    'error': {
      'color': 'red'
    },
    'info': {},
    'notice': {
      'color': 'magenta'
    },
    'spam': {
      'color': 'green',
      'faint': True
    },
    'success': {
      'color': 'green',
      'bold': True
    },
    'verbose': {
      'color': 'blue'
    },
    'warning': {
      'color': 'yellow'
    }
  }
  # By default the install() function installs a handler on the root logger,
  # this means that log messages from your code and log messages from the
  # libraries that you use will all show up on the terminal.
  # coloredlogs.install(level='DEBUG')

  # If you don't want to see log messages from libraries, you can pass a
  # specific logger object to the install() function. In this case only log
  # messages originating from that logger will show up on the terminal.
  coloredlogs.install(level='DEBUG', logger=logger)

  def log(text, color='white', level='INFO', **kwargs):
    extra = dict(sep=' -- ')
    level = 'ERROR' if color in ('red', 'r') else level
    level = 'WARNING' if color in ('yellow', 'y') else level
    level = 'SUCCESS' if color in ('green', 'g') else level
    level = 'NOTICE' if color in ('magenta', 'm') else level

    if isinstance(text, Exception):
      level = 'CRITICAL'
      error = text
      # text = 'ERROR: ' + str(error) + '\n' + ''.join(
      #   traceback.format_exception(
      #     etype=type(error), value=error, tb=error.__traceback__))
      text = 'ERROR: ' + ''.join(
        traceback.format_exception(
          etype=type(error),
          value=error,
          tb=error.__traceback__,
        ))

    elif color == 'white':
      text_ = str(text)
      if text_.startswith('~~'):
        level = 'CRITICAL'
        text = text_[2:]
      elif text_.startswith('~'):
        level = 'ERROR'
        text = text_[1:]
      if text_.startswith('-'):
        level = 'WARNING'
        text = text_[1:]
      if text_.startswith('+'):
        level = 'SUCCESS'
        text = text_[1:]
      if text_.startswith('*'):
        level = 'NOTICE'
        text = text_[1:]

    if level == 'CRITICAL':
      logger.critical(text, extra=extra)
    elif level == 'ERROR':
      logger.error(text, extra=extra)
    elif level == 'WARNING':
      logger.warning(text, extra=extra)
    elif level == 'DEBUG':
      logger.debug(text, extra=extra)
    elif level == 'SUCCESS':
      logger.success(text, extra=extra)
    elif level == 'NOTICE':
      logger.notice(text, extra=extra)
    else:
      logger.info(text, extra=extra)

  elog = lambda text, show_time=True: log(text, level='ERROR', show_time=show_time)
  slog = lambda text, show_time=True: log(text, level='SUCCESS', show_time=show_time)

except:

  def log(text, color='white', show_time=True, new_line=True, **kwargs):
    """Print to stdout"""
    if color_enabled:
      time_str = now_str() + Fore.MAGENTA + ' -- ' if show_time else ''
      color_map = dict(
        red=Fore.RED,
        blue=Fore.BLUE,
        green=Fore.GREEN,
        yellow=Fore.YELLOW,
        magenta=Fore.MAGENTA,
        white=Fore.WHITE,
      )
      text = str(text)
      if color == 'white':
        if text.startswith('-'):
          color = 'yellow'
          text = text[1:]
        if text.startswith('+'):
          color = 'green'
          text = text[1:]
        if text.startswith('*'):
          color = 'magenta'
          text = text[1:]
      line = '{}{}{}'.format(
        time_str, color_map[color.lower()] + str(text) + Style.RESET_ALL, '\n'
        if new_line else '')
    else:
      time_str = now_str() + ' -- ' if show_time else ''
      line = '{}{}{}'.format(time_str, text, '\n' if new_line else '')

    sys.stdout.write(line)
    sys.stdout.flush()

  elog = lambda text, show_time=True: log(text, color='red', show_time=show_time)
  slog = lambda text, show_time=True: log(text, color='green', show_time=show_time)


def kill_pid(pid, signum=signal.SIGTERM):
  if os.getpid() == pid:
    # log('-Cannot Kill own PID ({})'.format(pid))
    return
  try:
    os.kill(pid, signum)
  except ProcessLookupError:
    pass


def register_pid(pid_file,
                 pid=None,
                 kill_if_running=False,
                 clean_atexit=True,
                 exit_queue=None):
  "Register PID and verify already running state"
  from .diskio import write_file
  pid = pid if pid else os.getpid()
  cleanup_pid(pid_file, kill_if_running)
  write_file(pid_file, str(pid))
  signal_exit = lambda signum, frame: exit_queue.put(True)
  if clean_atexit:
    if exit_queue:
      # signal.signal(signal.SIGINT, clean_up_func)
      signal.signal(signal.SIGTERM, signal_exit)
    atexit.register(cleanup_pid, pid_file, kill_if_running=True)


def cleanup_pid(pid_file, kill_if_running=False, exit_queue=None):
  "Cleans up old PID file located in home folder"
  from .diskio import read_file
  import psutil

  if os.path.exists(pid_file):
    old_pid = int(read_file(pid_file).rstrip())
    if not kill_if_running and psutil.pid_exists(old_pid):
      raise Exception('PID {} still running! -> {}'.format(old_pid, pid_file))
    kill_pid(old_pid, 9)
    os.remove(pid_file)
    # log('Cleaned Up Old PID {}'.format(old_pid))


def get_exception_message(append_message='', raw=False):
  """Returns the exception message as a formatted string"""
  import linecache
  import traceback

  exc_type, exc_obj, tb = sys.exc_info()
  f = tb.tb_frame
  lineno = tb.tb_lineno
  filename = f.f_code.co_filename
  linecache.checkcache(filename)
  line = linecache.getline(filename, lineno, f.f_globals)
  message = '-' * 65 + '\n' + 'EXCEPTION IN ({}, LINE {} "{}"): {} \n---\n{}'.format(
    filename, lineno, line.strip(), exc_obj,
    traceback.format_exc()) + '\n' + append_message
  if raw: message = str(exc_obj)
  return message


def get_error_str(err):
  err_type = type(err).__name__
  err_msg = str(err)
  return '{}: {}'.format(err_type, err_msg)


time_start = now()


def get_elapsed_time(time_start=time_start):
  time_end = datetime.datetime.now()
  delta_time = divmod(
    (time_end - time_start).days * 86400 + (time_end - time_start).seconds, 60)
  return str(delta_time[0]) + ' mins ' + str(
    delta_time[1]) + ' seconds elapsed'


pprogress_stdt = now()
pprogress_updt = now()
pprogress_i = 1000


def pprogress(i, total, sec_intvl=0.2, text='Complete', show_time=True):
  "Print loop progress"
  global pprogress_updt, pprogress_stdt, pprogress_i
  pp = lambda: sys.stderr.write('\r{:,.1f}% [{} / {}] {}'.format(100.0*(i)/total, i, total, text))
  if i < pprogress_i:
    pprogress_stdt = now()

  if i == total:
    pp()
    print('')
    if show_time: print(get_elapsed_time(pprogress_stdt))
  elif (now() - pprogress_updt).total_seconds() > sec_intvl:
    pp()
    pprogress_updt = now()
  pprogress_i = i


def ptable(headers, rows, format='string', table=None):
  from prettytable import PrettyTable
  if table:
    table.clear_rows()
  else:
    table = PrettyTable(headers)

  for row in rows:
    table.add_row(row)

  return table.get_string()


### Profile ############################

from xutil.diskio import read_file, write_file, read_yaml, write_yaml


def save_profile(data):
  profl_path = os.getenv('PROFILE_YAML', get_home_path() + '/profile.yaml')
  # profl_path = get_home_path() + '/profile.new.yaml'
  write_yaml(profl_path, data)


def load_profile(raw_text=False, create_if_missing=False):
  if not os.getenv('PROFILE_YAML'):
    def_profl_path = get_home_path() + '/profile.yaml'
    templ_path = get_dir_path(__file__) + '/database/templates/profile.yaml'
    if not file_exists(def_profl_path) and create_if_missing:
      write_file(def_profl_path, read_file(templ_path))
    os.environ['PROFILE_YAML'] = def_profl_path
    # raise Exception("Env Var PROFILE_YAML is not set!")

  if raw_text:
    return read_file(os.getenv('PROFILE_YAML'))

  dict_ = read_yaml(os.getenv('PROFILE_YAML'))
  if 'environment' in dict_:
    for key in dict_['environment']:
      os.environ[key] = dict_['environment'][key]

  return dict_


def get_variables(profile=None):
  profile = profile if profile else get_profile()
  if 'variables' in profile:
    return profile['variables']
  else:
    return {}


def get_databases(profile=None):
  profile = profile if profile else get_profile()
  if 'databases' in profile:
    return profile['databases']
  else:
    raise Exception("Key 'databases' not in profile!")


def get_db_profile(db_name):
  dbs = get_databases()
  if db_name in dbs:
    return struct(dbs[db_name])
  else:
    raise Exception("Could not find Database '{}' in profile.".format(db_name))


## CLASSES & DATA OPS ##########################


class struct(dict):
  """ Dict with attributes getter/setter. """

  def __getattr__(self, name):
    return self[name]

  def __setattr__(self, name, value):
    self[name] = value


class State():
  """ State persisted.
  
    >>> from xutil.helpers import State, now
    >>> state = State()  # will create JSON file or load if exists
    >>> state.put(name='Mark', updated=now())
    >>> state.data['name'] = 'Derrick'  # overwrite 'Mark'
  """

  __checkpoint = 5  # auto-save every update count

  def __init__(self, name=None):
    self._name = name if name else get_file_name(__file__)
    self._path = name if '/' in name or '\\' in name else '{}/{}.state'.format(
      get_dir_path(), self._name)

    self.__chkpnt_cnt = 0
    self.__deleted = False

    self.data = {}

    if Path(self._path).exists():
      self.load()
    else:
      self.save()

    # save at program termination
    atexit.register(self.save)

  def put(self, **kwargs):
    "Alter data"
    for k in kwargs:
      self.data[k] = kwargs[k]
      self.__chkpnt_cnt += 1

    if self.__chkpnt_cnt >= self.__checkpoint:
      self.save()
      self.__chkpnt_cnt = 0

  def save(self):
    "Persist to disk"
    if not self.__deleted:
      with open(self._path, 'w') as file:
        json.dump(self.data, file, cls=MyJSONEncoder)

  def load(self):
    "Load from disk"
    if not self.__deleted:
      with open(self._path) as file:
        self.data = json.load(file, object_hook=MyJSONdateHook)
    else:
      print('State has been deleted.')

  def delete(self):
    "Delete from disk"
    os.remove(self._path)
    self.__deleted = True



class MyJSONEncoder(json.JSONEncoder):
  def default(self, o):
    if isinstance(o, datetime.datetime) or isinstance(o, datetime.date):
      # return o.isoformat()
      return str(o)
    elif isinstance(o, bytes):
      return o.decode(errors='ignore')
    elif isinstance(o, Decimal):
      return float(o)
    try:
      val = json.JSONEncoder.default(self, o)
    except:
      val = str(o)
    return val


def MyJSONdateHook(json_dict):
  for (key, value) in json_dict.items():
    try:
      json_dict[key] = datetime.datetime.strptime(value,
                                                  "%Y-%m-%d %H:%M:%S.%f")
    except:
      pass
  return json_dict


def str_format(text, data, enc_char):
  """This takes a text template, and formats the encapsulated occurences with data keys
  Example: if text = 'My name is $$name$$', provided data = 'Bob' and enc_char = '$$'.
    the returned text will be 'My name is Bob'.
  """
  for key in data:
    val = data[key]
    text = text.replace('{}{}{}'.format(enc_char, str(key), enc_char),
                        str(val))
  return text


def str_rmv_indent(text):
  """This removes the indents from a string"""
  # return text
  if not text: return text
  matches = list(re.finditer(r'^ *', text.strip(), re.MULTILINE))
  if not matches: return text
  min_indent = min([len(m.group()) for m in matches])
  regex = r"^ {" + str(min_indent) + r"}"
  new_text = re.sub(regex, '', text, 0, re.MULTILINE)
  return new_text


def split(data, split_size):
  """Yield successive split_size-sized chunks from data (list or dataframe)."""
  if isinstance(data, list):
    for i in range(0, len(data), split_size):
      yield data[i:i + split_size]
  else:
    for i in range(0, len(data), split_size):
      yield data.iloc[i:i + split_size]


def isnamedtupleinstance(x):
  t = type(x)
  b = t.__bases__
  if len(b) != 1 or b[0] != tuple: return False
  f = getattr(t, '_fields', None)
  if not isinstance(f, tuple): return False
  return all(type(n) == str for n in f)


def get_process_data(p):
  mem_info = p.memory_info()
  cpu_times = p.cpu_times()
  rec = p.as_dict()
  # rec = dict(
  #   date_time=now(),
  #   host_name=host_name,
  #   pid=p.pid,
  #   # cmdline = str(p.cmdline()),
  #   cmdline=' '.join(p.cmdline()).replace('\n', ' ').replace('|', ' '),
  #   cpu_percent=_rec['cpu_percent'] or 0,
  #   cpu_times_user=cpu_times.user,
  #   cpu_times_system=cpu_times.system,
  #   cpu_times_children_user=cpu_times.children_user,
  #   cpu_times_children_system=cpu_times.children_system,
  #   create_time=p.create_time(),
  #   # cwd = p.cwd(),
  #   # exe = p.exe(),
  #   # io_counters = p.io_counters(),
  #   memory_info_rss=mem_info.rss,
  #   memory_info_vms=mem_info.vms,
  #   # memory_info_shared=mem_info.shared,
  #   memory_percent=p.memory_percent(),
  #   name=p.name().replace('\n', ' '),
  #   username=p.username().replace('\n', ' '),
  #   status=p.status().replace('\n', ' '),
  #   num_threads=p.num_threads(),
  # )
  return rec


class DictTree:
  def __init__(self, data, header_only=False, name_processor=None):
    self.data = data
    self.name_processor = name_processor
    self.get_keys_path(header_only=header_only)
    self.TRow = namedtuple(
      'TRow',
      'title action field_name field_type depth_level field_path is_null is_list len'
    )

  def get_keys_path(self, header_only=False):
    "Traverses nested dictionary and returns depth level and corresponding JMESPath search string"
    # http://jmespath.org/specification.html#examples
    # https://github.com/jmespath/jmespath.py
    from pyspark.sql import Row
    fields_parent = OrderedDict()
    DP = namedtuple('DictPath', 'level key path vtype field_name')
    name_processor = self.name_processor if self.name_processor else None

    def get_data_type(val):
      if isinstance(val, dict): return 'dict'
      if isinstance(val, list): return 'list'
      try:
        float(val)
        return 'number'
      except:
        try:
          dateutil.parser.parse(val)
          return 'datetime'
        except:
          return 'string'

    def get_field_name(key, parent):
      prefix = ''
      parent = parent.replace('"', '')

      if (parent.endswith('[*]') and len(parent.split('.')) > 1) or \
          (parent.endswith('[*]') and not parent.startswith('[*]')):
        prefix = parent.replace('[*]', '').split('.')[-1] + '_'

      if name_processor:
        prefix = name_processor(prefix)
        key = name_processor(key)

      f_name_new = f_name = prefix + key
      i = 0
      while f_name_new in fields_parent and fields_parent[f_name_new] != parent:
        i += 1
        f_name_new = f_name + str(i)
      fields_parent[f_name_new] = parent
      return f_name_new

    q = lambda s: '"' + s + '"'

    def get_paths(d1, level=1, parent=''):
      kks = set()

      if isinstance(d1, dict):
        if "Header" in d1:
          keys = ["Header"] + [k for k in d1 if k != "Header"]
        else:
          keys = list(d1)
        for key in keys:
          if key.startswith('@'): continue
          vt = get_data_type(d1[key])
          field_name = get_field_name(
            key, parent) if vt not in ('dict', 'list') else None
          path = '{}{}'.format(parent + '.' if parent else '', q(key))
          kks.add(DP(level, key, path, vt, field_name))
          kks = kks.union(get_paths(d1[key], level=level + 1, parent=path))
          if header_only and key == "Header": return kks
      elif isinstance(d1, list):
        key = '[*]'
        path = '{}{}'.format(parent if parent else '', key)
        if level == 1:
          vt = get_data_type(d1)
          kks.add(DP(level, key, path, vt, None))
        for item in d1:
          vt = get_data_type(item)
          kks = kks.union(get_paths(item, level=level + 1, parent=path))

      return kks

    self.paths = sorted(get_paths(self.data))
    self.v_paths = [r for r in self.paths if r.vtype not in ('dict', 'list')]
    self.v_lists = [r for r in self.paths if r.vtype in ('list')]
    self.fields = [
      r.field_name for r in self.paths if r.vtype not in ('dict', 'list')
    ]
    self.fields_dict = {
      r.field_name: r
      for r in self.paths if r.vtype not in ('dict', 'list')
    }
    self.Rec = Row(*self.fields)

    try:
      self.title = list(self.data)[0]
      self.action = self.get_value('Action', conv_date=False)
    except:
      pass

    return self.paths

  def search(self, path):
    from jmespath import search
    return search(path, self.data)

  def get_value(self, field, conv_date=True):
    val = self.search(self.fields_dict[field].path)
    if conv_date and self.fields_dict[field].vtype == 'date':
      try:
        val = dateutil.parser.parse(val)
      except:
        pass
    return val

  def get_field_table(self):
    "Create fields table"
    t_rows = []
    for field in self.fields:
      val = self.get_value(field)
      rec = dict(
        title=self.title,
        action=self.action,
        field_name=field,
        field_type=self.fields_dict[field].vtype,
        depth_level=self.fields_dict[field].level,
        field_path=self.fields_dict[field].path,
        is_null=0 if str(val).strip() else 1,
        is_list=1 if isinstance(val, list) else 0,
        len=len(str(val)),
      )

      t_rows.append(self.TRow(**rec))
    return t_rows

  def get_record(self):
    "Returns one record, omitting lists N:N relationships"
    return self.Rec(*[self.get_value(f) for f in self.fields])
