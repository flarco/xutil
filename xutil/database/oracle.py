import datetime

from xutil.database.base import DBConn

from xutil.helpers import (get_exception_message, now, log, struct,
                           is_gen_func, isnamedtupleinstance)
from xutil.diskio import read_yaml, write_jsonl, read_jsonl


class OracleConn(DBConn):
  "Oracle Connection"

  data_map = dict(
    string='VARCHAR2()',
    integer='NUMBER()',
    decimal='NUMBER()',
    date='DATE',
    datetime='DATE',
    # timestamp='TIMESTAMP(6)',
    timestamp='DATE',
    text='NCLOB',
  )

  truncate_datef = lambda self, f: 'trunc({})'.format(f)

  def set_variables(self):
    import cx_Oracle

    self.col_name_id = 2

    self.reverse_data_map = {
      cx_Oracle.NUMBER: 'integer',
      cx_Oracle.STRING: 'string',
      cx_Oracle.LONG_STRING: 'string',
      cx_Oracle.NCHAR: 'string',
      cx_Oracle.FIXED_CHAR: 'string',
      cx_Oracle.DATETIME: 'datetime',
      cx_Oracle.TIMESTAMP: 'datetime',
      cx_Oracle.NATIVE_FLOAT: 'double',
      cx_Oracle.LOB: 'text',
      cx_Oracle.CLOB: 'text',
      cx_Oracle.BLOB: 'text',
      cx_Oracle.NCLOB: 'text',
      cx_Oracle.BINARY: 'text',
    }

    self.error_msg = dict(table_not_exist='table or view does not exist', )

  def _fix_f_name(self, fname):
    "Check length or field, or something else"
    if len(fname) > 30:
      fname = fname[:30]
    return fname

  def connect(self):
    "Connect / Re-Connect to Database"
    import cx_Oracle

    # get_conn_str = lambda cred: '{}/{}@{}'.format(cred.user, cred.password, cred.name)

    def get_conn_str(cred):
      if 'service' in cred:
        dns_str = cx_Oracle.makedsn(
          cred.host, cred.port, service_name=cred.service)
      elif 'sid' in cred:
        dns_str = cx_Oracle.makedsn(cred.host, cred.port, sid=cred.sid)
      return '{}/{}@{}'.format(cred.user, cred.password, dns_str)

    conn_str = get_conn_str(struct(self._cred)) if isinstance(
      self._cred, dict) else self._cred
    self.connection = cx_Oracle.connect(conn_str)
    self.cursor = None

    self.connection.autocommit = True
    self.name = self._cred['name'] if isinstance(
      self._cred, dict) else self.connection.tnsentry
    self.username = self.connection.username

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import oracle, postgresql
    return postgresql

  def create_engine(self, conn_str=None, echo=False):
    from cx_Oracle import makedsn
    import sqlalchemy
    if conn_str:
      conn_str = 'oracle+cx_oracle://' + conn_str
    else:
      cred = struct(self._cred)
      if 'service' in cred:
        dns_str = makedsn(cred.host, cred.port, service_name=cred.service)
      elif 'sid' in cred:
        dns_str = makedsn(cred.host, cred.port, sid=cred.sid)
      else:
        dns_str = makedsn(cred.host, cred.port)

      conn_str = (
        'oracle+cx_oracle://{user}:{password}@' + dns_str).format(**cred)

    self.engine = sqlalchemy.create_engine(conn_str, pool_size=10, echo=echo)

    return self.engine

  def insert(self,
             table,
             data,
             field_types=None,
             commit=True,
             fields=[],
             echo=False):
    "Insert records of namedtuple or dicts"
    import cx_Oracle

    s_t = datetime.datetime.now()
    cx_data_map = dict(
      string=cx_Oracle.STRING,
      integer=cx_Oracle.NUMBER,
      decimal=cx_Oracle.NUMBER,
      date=cx_Oracle.DATETIME,
      datetime=cx_Oracle.DATETIME,
      text=cx_Oracle.CLOB,
    )

    # if not len(data):
    #   return False

    row = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(row) else 'dict'
    if not fields:
      fields = row._fields if mode == 'namedtuple' else sorted(row.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    self.connection.autocommit = False
    cursor = self.get_cursor()
    sql = self.template('core.insert').format(
      table=table,
      options=self.template('core.insert_option'),
      names=', \n'.join([self._fix_f_name(f) for f in fields]),
      values=', \n'.join([':' + str(val) for val in values]),
    )

    if field_types:
      input_sizes = {
        self._fix_f_name(f): cx_data_map[field_types[f][0]]
        for f in field_types
      }
      print(len(input_sizes))
      print(input_sizes)
      cursor.setinputsizes(**input_sizes)

    cursor.prepare(sql)

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
          cursor.executemany(None, batch)
          counter += len(batch)
      else:
        # cursor.bindvars = None
        cursor.executemany(None, data)
        counter += len(data)

      if commit:
        self.connection.commit()
      else:
        return counter

    except Exception as e:
      log(Exception('Error for SQL: ' + sql))
      raise e

    finally:
      self.connection.autocommit = True

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)
    if echo:
      log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
        counter, table, mins, rate))
    return counter

  def replace(self,
              table,
              data,
              pk_fields,
              field_types=None,
              commit=True,
              echo=True):
    "Insert/Update records of namedtuple or dicts"
    import cx_Oracle

    s_t = datetime.datetime.now()
    cx_data_map = dict(
      string=cx_Oracle.STRING,
      integer=cx_Oracle.NUMBER,
      decimal=cx_Oracle.NUMBER,
      date=cx_Oracle.DATETIME,
      datetime=cx_Oracle.DATETIME,
      text=cx_Oracle.CLOB,
    )

    if not len(data):
      return False

    row = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(row) else 'dict'
    fields = row._fields if mode == 'namedtuple' else sorted(row.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    self.connection.autocommit = False
    cursor = self.get_cursor()
    pk_fields_set = set(pk_fields)
    sql = self.template('core.replace').format(
      table=table,
      name_values=',\n'.join(
        [':{} as {}'.format(values[i], f) for i, f in enumerate(fields)]),
      src_tgt_condition='\nAND '.join(
        ['src.{f} = tgt.{f}'.format(f=f) for f in pk_fields]),
      set_fields=',\n'.join([
        'tgt.{f} = src.{f}'.format(f=f) for i, f in enumerate(fields)
        if f not in pk_fields_set
      ]),
      names=',\n'.join(['tgt.{f}'.format(f=f) for f in fields]),
      values=',\n'.join(['src.{f}'.format(f=f) for f in fields]),
    )

    if field_types:
      input_sizes = {
        self._fix_f_name(f): cx_data_map[field_types[f][0]]
        for f in field_types
      }
      print(len(input_sizes))
      print(input_sizes)
      cursor.setinputsizes(**input_sizes)

    cursor.prepare(sql)

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
          cursor.executemany(None, batch)
          counter += len(batch)
      else:
        # cursor.bindvars = None
        cursor.executemany(None, data)
        counter += len(data)

      if commit:
        self.connection.commit()
      else:
        return counter

    except Exception as e:
      log(Exception('Error for SQL: ' + sql))
      raise e

    finally:
      self.connection.autocommit = True

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)
    if echo:
      log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
        counter, table, mins, rate))
    return counter

  def _concat_fields(self, fields, as_text=False):
    if as_text:
      fields = ['cast({} as varchar2(4000))'.format(f) for f in fields]
    return ' || '.join(fields)
