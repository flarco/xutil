import os
from xutil.database.base import DBConn
from xutil.helpers import get_dir_path, slog


class JdbcConn(DBConn):
  """Any JDBC Connection String"""

  def connect(self):
    "Connect / Re-Connect to Database"
    import jaydebeapi

    cred = self._cred
    profile = self.profile
    jar_path = get_jar_path(cred.type, profile)

    os.environ['CLASSPATH'] = '{}:{}'.format(
      jar_path, os.environ['CLASSPATH'] if 'CLASSPATH' in os.environ else '')

    self.connection = jaydebeapi.connect(
      profile['drivers'][cred.type]['class'],
      cred.url,
      [cred.user, cred.password],
      jar_path,
    )
    self.cursor = None

    # self.connection.autocommit = True
    self.name = cred.type
    self.username = cred.user if cred else ''

    cursor = self.get_cursor()

  def set_variables(self):
    if self.type == 'sqlserver':
      self.truncate_datef = lambda f: 'CONVERT(DATE, {})'.format(f)
    if self.type == 'oracle':
      self.truncate_datef = lambda f: 'trunc({})'.format(f)


def get_jar_path(db_type, profile):
  from xutil.diskio import get_zip_path
  from zipfile import ZipFile
  from pathlib import Path
  import shutil

  tmp_folder = profile['variables']['tmp_folder']

  jar_path = profile['drivers'][db_type]['path']

  if Path(jar_path).exists():
    return jar_path

  # try relative path
  jar_path = '{}/{}'.format(
    get_dir_path(__file__), profile['drivers'][db_type]['path'])

  if '.zip' in jar_path.lower():
    new_jar_path = '{}/{}'.format(tmp_folder, Path(jar_path).name)
    if not Path(new_jar_path).exists():
      zip_path = get_zip_path(jar_path)

      file_obj_path = jar_path.replace(zip_path + '/', '')
      with ZipFile(zip_path) as zip_f:
        with zip_f.open(file_obj_path) as zf, open(new_jar_path, 'wb') as f:
          shutil.copyfileobj(zf, f)

    jar_path = new_jar_path

  if not Path(jar_path).exists():
    raise Exception('Could not find JAR path "{}"'.format(jar_path))

  return jar_path
