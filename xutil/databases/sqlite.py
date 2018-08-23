import sys, datetime
from xutil.databases.base import DBConn
from xutil.helpers import get_exception_message, log


class SQLiteConn(DBConn):
  "SQLite Connection"

  data_map = dict(
    string='TEXT',
    integer='NUMBER',
    number='NUMBER',
    decimal='REAL',
    date='TEXT',
    datetime='TEXT',
    text='BLOB',
  )

  def set_variables(self):
    self.error_msg = dict(table_not_exist='table or view does not exist', )

    self.sql_template = dict(
      drop_table='drop table {} cascade constraints',
      insert="INSERT {options} INTO {table} ({names}) VALUES ({values})",
      replace='''REPLACE INTO {table} ({names})
      VALUES({values})''',
      create_table="create table if not exists {table}\n({col_types})",
      create_index="create index {index} on {table} ({cols})",
      insert_option='',
    )

  def connect(self):
    "Connect / Re-Connect to Database"
    import sqlite3
    self.connection = sqlite3.connect(self._cred.database)
    self.cursor = None

    # self.connection.autocommit = True
    self._cred.name = 'sqlite3'
    self._cred.user = ''

    cursor = self.get_cursor()
    cursor.execute('PRAGMA temp_store=MEMORY')
    cursor.execute('PRAGMA journal_mode=MEMORY')

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import sqlite
    return sqlite

  def create_engine(self, conn_str=None):
    import sqlalchemy
    if not conn_str:
      conn_str = ('sqlite:///' + self._cred.database)

    self.engine = sqlalchemy.create_engine(conn_str)

    return self.engine

  def replace(self, table, data, pk_fields=None, commit=True, echo=True):
    "Upsert data into database"
    s_t = datetime.datetime.now()

    if not len(data):
      return False

    row = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(row) else 'dict'
    fields = row._fields if mode == 'namedtuple' else sorted(row.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    # self.connection.autocommit = False
    cursor = self.get_cursor()

    sql = self.sql_template['replace'].format(
      table=table,
      names=', '.join(fields),
      values=', '.join(['?' for val in values]),
    )

    try:
      counter = 0
      if is_gen_func(data):
        batch = [row]

        for row in data:
          batch.append(row)
          if len(batch) == self.batch_size:
            cursor.executemany(sql, batch)
            counter += len(batch)
            batch = []

        if len(batch):
          cursor.executemany(sql, batch)
          counter += len(batch)
      else:
        # cursor.bindvars = None
        cursor.executemany(sql, data)
        counter += len(data)

      if commit:
        self.connection.commit()
      else:
        return counter

    except Exception as e:
      message = get_exception_message().lower()
      log(get_exception_message())
      log(sql)
      raise e

    # finally:
    #   self.connection.autocommit = True

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)
    if echo:
      log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
        counter, table, mins, rate))
    return counter

  def insert(self, table, data):
    "Insert records of namedtuple or dicts"
    if not data:
      return False

    s_t = datetime.datetime.now()
    row = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(row) else 'dict'
    fields = row._fields if mode == 'namedtuple' else sorted(row.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    # self.connection.autocommit = False
    cursor = self.get_cursor()
    sql = self.sql_template['insert'].format(
      table=table,
      options=self.sql_template['insert_option'],
      names=', '.join(fields),
      values=', '.join(['?' for val in values]),
    )
    # cursor.prepare(sql)
    try:
      counter = 0
      if is_gen_func(data):
        batch = [row]

        for row in data:
          batch.append(row)
          if len(batch) == self.batch_size:
            cursor.executemany(None, batch)
            counter += len(batch)
            batch = []

        if len(batch):
          cursor.executemany(sql, batch)
          counter += len(batch)
      else:
        cursor.executemany(sql, data)
        counter += len(data)

      self.connection.commit()
      log("Inserted {} records into table '{}'.".format(counter, table))

    except Exception as e:
      message = get_exception_message().lower()
      log(get_exception_message())
      raise e

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)
    log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
      counter, table, mins, rate))
    return counter
