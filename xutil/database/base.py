# Database Lib
"""
Oracle
PostGresSQL
SQLite
SQLServer
Hive
Spark
"""
import os, datetime, pandas, time, re
from collections import namedtuple, OrderedDict

import jmespath, typing
import sqlalchemy
from multiprocessing import Queue, Process

from xutil.helpers import (
  log,
  elog,
  slog,
  get_exception_message,
  struct,
  now,
  get_databases,
  get_dir_path,
  get_profile,
  get_variables,
  file_exists,
  str_rmv_indent,
  ptable,
  make_rec,
)
from xutil.diskio import read_yaml, write_csvs

conns = {}

_fwk = lambda k, v: "{} = '{}'".format(k, v)
_fw = lambda sep, **kws: sep.join([_fwk(k, v) for k, v in kws.items()])  # Format WHERE
fwa = lambda **kws: _fw(' and ', **kws)  # Format WHERE AND
fwo = lambda **kws: _fw(' or ', **kws)  # Format WHERE OR


class DBConn(object):
  """Base class for database connections"""

  _fix_f_name = lambda self, f: f

  def __init__(self, conn_dict, profile=None, echo=False):
    "Inititate connection"
    self._cred = struct(conn_dict)
    self.type = self._cred.type
    self.engine = None
    self.profile = profile
    self.batch_size = 10000
    self.fetch_size = 20000
    self.connect()
    self.last_connect = now()
    self.echo = echo

    # Base Template
    template_base_path = '{}/database/templates/base.yaml'.format(
      get_dir_path())
    self.template_dict = read_yaml(template_base_path)

    # Specific Type Template
    template_path = '{}/database/templates/{}.yaml'.format(
      get_dir_path(), self.type)
    temp_dict = read_yaml(template_path)

    for key1 in temp_dict:
      # Level 1
      if isinstance(temp_dict[key1], dict):
        if key1 not in self.template_dict:
          self.template_dict[key1] = temp_dict[key1]

        # Level 2
        for key2 in temp_dict[key1]:
          # Always Overwrite
          self.template_dict[key1][key2] = temp_dict[key1][key2]
      else:
        # Level 1 Non-Dict Overwrite
        self.template_dict[key1] = temp_dict[key1]

    self.variables = self.template('variables')

    if os.getenv('PROFILE_YAML'):
      other_vars = get_variables()
      for key in other_vars:
        self.variables[key] = other_vars[key]

    self.tmp_folder = self.variables['tmp_folder']
    self.set_variables()

    if echo:
      log("Connected to {} as {}".format(self._cred.name, self._cred.user))

  def connect(self):
    """Connect  to Database"""
    raise Exception("Method 'connect' is not implemented!")

  def reconnect(self, min_tresh=0):
    """Re-Connect to Database if minute threshold reached"""
    if (now() - self.last_connect).total_seconds() > min_tresh * 60:
      log('Reconnecting...')
      self.connect()
      self.last_connect = now()

  def set_variables(self):
    """Set custom variables"""
    raise Exception("Method 'set_variables' is not implemented!")

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    raise Exception("Method 'get_dialect' is not implemented!")

  def get_engine(self, echo=False):
    import sqlalchemy
    if not self.engine:
      self.create_engine(echo=self.echo)
    self.engine_inspect = sqlalchemy.inspect(self.engine)
    return self.engine

  def check_pk(self, table, fields):
    "Check Primary key to ensure there are not duplicates"
    if 'where' in fields.lower():
      fields, where_clause = fields.lower().split('where')
      where_clause = 'where ' + where_clause
    else:
      where_clause = ''
    sql = '''
    select
      '{table}' as table,
      case when count(1) = count({fields}) then 'PASS' else 'FAIL' end as pk_result
    from {table}
    {where_clause}
    '''.format(
      table=table,
      fields=fields,
      where_clause=where_clause,
    )
    data = self.select(sql, echo=False)
    headers = self.get_cursor_fields()
    print(ptable(headers, data))
    if data[0].pk_result == 'FAIL':
      raise (Exception('PK Text failed for table "{}" with fields "{}"'.format(
        table, fields)))

  def _do_execute(self, sql, cursor=None):
    cursor = cursor if cursor else self.cursor
    cursor.execute(sql)
    self._fields = self.get_cursor_fields()

  def execute_multi(self,
                    sql,
                    dtype='namedtuple',
                    limit=None,
                    echo=True,
                    query_name='Record',
                    log=log):
    """
    Execute multiple SQL statements separtated by ';'. Returns a generator.
    Example:
      for fields, rows in conn.execute(sql):
        print(fields)
        print(len(rows))
    """

    self.reconnect(min_tresh=10)

    cursor = self.get_cursor()
    data = None
    fields = None
    rows = []
    message_mapping = {
      'drop ': 'Dropping {}.',
      'truncate ': 'Truncating {}.',
      'select ': 'Selecting {}.',
      'create ': 'Creating {}.',
      'insert ': 'Inserting {}.',
      'alter ': 'Altering {}.',
      'update ': 'Updating {}.',
      'delete ': 'Deleting {}.',
      'exec ': 'Calling Procedure {}.',
      'grant ': 'Granting {}.',
    }

    sqls = sql.split(';')

    for sql in sqls:
      if not sql.strip(): continue

      sql_ = sql.strip().lower()

      for word, message in message_mapping.items():
        if sql_.startswith(word):
          if echo:
            log(
              message.format(' '.join(
                sql_.splitlines()[0].split()[1:3]).upper()))
          break

      # Call procedure with callproc
      if sql_.startswith('exec '):
        procedure = sql_[5:].split('(')[0]
        args = sql_[5:].split('(')[1][:-1].replace("'", '').split(',')
        args = [a.strip() for a in args]
        cursor.callproc(procedure, args)
        continue

      try:
        rows = self.select(
          sql,
          rec_name=query_name,
          dtype=dtype,
          limit=limit,
          echo=echo,
          log=log)
        fields = self._fields

        if '-- pk_test:' in sql.lower() and sql_.startswith('create'):
          sql_lines = sql_.splitlines()
          regexp = r'create\s+table\s+(\S*)[\sa-zA-Z\d]+ as'
          table = re.findall(regexp, sql_lines[0])[0]
          line = [
            l for l in sql_lines if l.strip().lower().startswith('-- pk_test:')
          ][0]
          fields = line.split(':')[-1]
          self.check_pk(table, fields)

      except Exception as e:
        message = get_exception_message().lower()

        if sql_.startswith(
            'drop ') and self.error_msg['table_not_exist'] in message:
          log("WARNING: Table already dropped.")
        else:
          log(get_exception_message())
          log(sql)
          raise e

      if not fields: fields = []

      yield fields, rows

  def execute(self,
              sql,
              dtype='tuple',
              limit=None,
              echo=True,
              query_name='Record',
              log=log):
    """Execute SQL, return last result"""
    results = list(
      self.execute_multi(
        sql=sql,
        dtype=dtype,
        limit=limit,
        echo=echo,
        query_name=query_name,
        log=log))
    return results[-1]

  def insert(self, table, data, echo=False):
    """Insert records of namedtuple or dicts"""
    from sqlalchemy import MetaData, Table
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.inspection import inspect

    schema, table_name = self._split_schema_table(table)

    engine = self.get_engine()

    metadata = MetaData(schema=schema)
    metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    # get list of fields making up primary key
    # primary_keys = [key.name for key in inspect(table_name).primary_key]

    # assemble base statement
    dialect = self.get_dialect()
    # statement = dialect.insert(table).values(data)
    statement = table.insert().values(data)

    # define dict of non-primary keys for updating
    # update_dict = {
    #     c.name: c
    #     for c in statement.excluded
    #     if not c.primary_key
    # }

    # assemble new statement with 'on conflict do update' clause
    # update_stmt = statement.on_conflict_do_update(
    #     index_elements=primary_keys,
    #     set_=update_dict,
    # )

    # execute
    with engine.connect() as conn:
      result = conn.execute(statement)
      return result

  def _exec_statement_records(self, schema, table_name, data, stmt):
    """Insert records of namedtuple or dicts"""
    from sqlalchemy import MetaData, Table
    from sqlalchemy.inspection import inspect

    schema, table_name = self._split_schema_table(table)

    engine = self.get_engine()

    metadata = MetaData(schema=schema)
    metadata.bind = engine

    table = Table(table_name, metadata, schema=schema, autoload=True)

    # execute
    with engine.connect() as conn:
      result = conn.execute(stmt)
      return result

  def drop_table(self, table, log=log):
    "Drop table"
    cursor = self.get_cursor()

    try:
      sql = self.template('core.drop_table').format(table)
      self._do_execute(sql)
    except Exception as E:
      message = get_exception_message().lower()
      if self.template('error_filter.table_not_exist') in message:
        if self.echo:
          log('Table "{}" already dropped.'.format(table))
      else:
        raise E

  def create_table(self, table, field_types, drop=False, log=log):
    "Create table"

    cursor = self.get_cursor()

    if drop:
      self.drop_table(table, log=log)

    new_ftypes = OrderedDict()
    for f in field_types:
      ftype, max_len, dec_len = field_types[f]
      if dec_len:
        suff = '({},{})'.format(max_len, dec_len)
      elif max_len:
        suff = '({})'.format(max_len)
      else:
        suff = ''

      new_ftypes[f] = self.template('general_type_map')[ftype].replace(
        '()', suff)

    field_types_str = ', \n'.join([
      self._fix_f_name(field) + ' ' + new_ftypes[field] for field in new_ftypes
    ])

    sql = self.template('core.create_table').format(
      table=table,
      col_types=field_types_str,
    )

    # log('Creating table: \n' + sql))
    try:
      self._do_execute(sql)
    except Exception as e:
      log(get_exception_message())
      log(sql)
      raise e

    log('Created table "{}"'.format(table))

  def get_cursor(self):
    "Instantiate connection cursor"
    if self.cursor is not None:
      self.cursor.close()

    self.cursor = self.connection.cursor()

    return self.cursor

  def get_cursor_fields(self, as_dict=False, native_type=True):
    "Get fields of active Select cursor"
    fields = OrderedDict()
    if self.cursor.description == None:
      return []

    for f in self.cursor.description:
      f_name = f[0].lower()
      if as_dict:
        if native_type:
          f_type = f[1]
        else:
          f_type = self.reverse_data_map[f[1]]

          # assign floa/double as needed
          if 'cx_Oracle.NUMBER' in str(f[1]):
            if f[4] and f[4] > 11: f_type = 'long'
            if f[5] and f[5] > 0: f_type = 'double'

        fields[f_name] = f_type
      else:
        fields[f_name] = None

    if as_dict:
      return fields
    else:
      return list(fields.keys())

  def stream(self,
             sql,
             rec_name='Record',
             dtype='namedtuple',
             yield_batch=False,
             echo=True):
    "Stream Select from SQL, yield records as they come in"
    self.reconnect(min_tresh=10)
    if echo: log("Streaming SQL for '{}'.".format(rec_name))

    self.get_cursor()
    log('Execute <-')
    self.cursor.arraysize = self.fetch_size
    self.cursor.itersize = self.fetch_size

    try:
      self._do_execute(sql)
    except Exception as e:
      log(e)
      log(sql)
      raise e

    log('Fields <-')
    self.field_types = self.get_cursor_fields(as_dict=False, native_type=False)
    fields = [f for f in self.field_types]

    if dtype == 'tuple':
      make_rec = lambda row: row
    else:
      Record = namedtuple(rec_name.replace(' ', '_').replace('.', '_'), fields)
      make_rec = lambda row: Record(*row)

    self._stream_counter = 0

    log('Collect <-')

    while True:
      rows = self.cursor.fetchmany(self.fetch_size)
      log(' -> {}'.format(len(rows)))
      if rows:
        if yield_batch:
          batch = [make_rec(r) for r in rows]
          self._stream_counter += len(batch)
          yield batch
        else:
          for row in rows:
            self._stream_counter += 1
            yield make_rec(row)
      else:
        break

    # log('Stream finished at {} records.'.format(self._stream_counter))

  def select(self,
             sql,
             rec_name='Record',
             dtype='namedtuple',
             limit=None,
             echo=True,
             retrying=False,
             log=log):
    "Select from SQL, return list of namedtuples"
    # if echo: log("Running SQL for '{}'.".format(rec_name))

    self.reconnect(min_tresh=10)
    s_t = datetime.datetime.now()
    cursor = self.get_cursor()

    try:
      self._do_execute(sql)
      fields = self._fields = self.get_cursor_fields()
      if not fields: return []

      if dtype == 'namedtuple':
        Record = namedtuple(
          rec_name.replace(' ', '_').replace('.', '_'), fields)
        if limit:
          data = [Record(*row) for row in cursor.fetchmany(limit)]
        else:
          data = [Record(*row) for row in cursor.fetchall()]

      elif dtype == 'tuple':
        if limit:
          data = [row for row in cursor.fetchmany(limit)]
        else:
          data = [row for row in cursor.fetchall()]

      elif dtype == 'dataframe':
        if limit:
          data = pandas.DataFrame(
            [row for row in cursor.fetchmany(limit)], columns=fields)
        else:
          data = pandas.DataFrame(
            [row for row in cursor.fetchall()], columns=fields)
      else:
        raise (Exception('{} is not recongnized.'.format(dtype)))

    except Exception as e:
      log(e)
      log(sql)
      raise e

    secs = (datetime.datetime.now() - s_t).total_seconds()
    rate = round(len(data) / secs, 1)
    if echo:
      log(" >>> Got {} rows in {} secs [{} r/s].".format(
        len(data), secs, rate))
    return data

  def _split_schema_table(self, obj):
    schema, table = obj.split('.') if '.' in obj else (self.username, obj)
    return schema, table

  def template(self, template_key_str):
    val = jmespath.search(template_key_str, self.template_dict)
    if isinstance(val, str):
      val = str_rmv_indent(val)
    return val

  def get_schemas(self, echo=True):
    "Get list of schemas."
    Rec = namedtuple('Schemas', 'schema')

    sql_tmpl = self.template('metadata.schemas')
    if sql_tmpl:
      schemas = [r[0] for r in self.select(sql_tmpl)]
    else:
      # http://docs.sqlalchemy.org/en/rel_0_9/core/reflection.html#sqlalchemy.engine.reflection.Inspector.get_schemas
      self.get_engine(echo=echo)
      schemas = self.engine_inspect.get_schema_names()

    rows = [Rec(s) for s in schemas]
    return rows

  def get_objects(self, schema, object_type='ALL', echo=True):
    "Get metadata for objects. object_type in 'ALL', 'TABLE', 'VIEW'"
    Rec = namedtuple('Table', 'schema object_name object_type')

    def get_rec(object_name, object_type):
      r_dict = dict(
        schema=schema, object_name=object_name, object_type=object_type)
      return Rec(**r_dict)

    if object_type == 'ALL':
      table_rows = self.get_tables(schema)
      rows = [get_rec(r.table, 'TABLE') for r in sorted(table_rows)]
      view_rows = self.get_views(schema)
      rows += [get_rec(r.view, 'VIEW') for r in sorted(view_rows)]
    elif object_type == 'TABLE':
      table_rows = self.get_tables(schema)
      rows = [get_rec(r.table, 'TABLE') for r in sorted(table_rows)]
    elif object_type == 'VIEW':
      view_rows = self.get_views(schema)
      rows += [get_rec(r.view, 'VIEW') for r in sorted(view_rows)]
    else:
      raise Exception('Object type "{}" not supported!'.format(object_type))

    return rows

  def get_tables(self, schema, echo=True):
    "Get metadata for tables."
    Rec = namedtuple('Table', 'schema table')

    def get_rec(table):
      r_dict = dict(schema=schema, table=table)
      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.tables')
    if sql_tmpl:
      tables = self.select(sql_tmpl.format(schema=schema))
      if hasattr(self, '_std_get_tables'):
        tables = self._std_get_tables(schema, tables)
    else:
      self.get_engine(echo=echo)
      tables = self.engine_inspect.get_table_names(schema)

    rows = [get_rec(t) for t in sorted(tables)]
    return rows

  def get_views(self, schema, echo=True):
    "Get metadata for views."
    Rec = namedtuple('View', 'schema view')

    def get_rec(view):
      r_dict = dict(schema=schema, view=view)
      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.views')
    if sql_tmpl:
      views = [r[0] for r in self.select(sql_tmpl.format(schema=schema))]
    else:
      self.get_engine(echo=echo)
      views = self.engine_inspect.get_view_names(schema)

    rows = [get_rec(v) for v in sorted(views)]
    return rows

  def get_columns(self, obj, object_type=None, echo=False):
    "Get column metadata for table"
    Rec = namedtuple(
      'Columns',
      'schema table column_name type column_order nullable default autoincrement'
    )
    schema, table = self._split_schema_table(obj)

    def get_rec(r_dict, column_order):
      r_dict['schema'] = schema
      r_dict['table'] = table
      r_dict['column_name'] = r_dict['name']
      r_dict['type'] = str(r_dict['type'])
      r_dict['column_order'] = column_order
      del r_dict['name']

      if 'attrs' in r_dict:
        del r_dict['attrs']

      if '(' in r_dict['type']:
        r_dict['type'] = r_dict['type'].split('(')[0]

      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.columns')
    if sql_tmpl:
      rows = self.select(sql_tmpl.format(table=table, schema=schema))
      if hasattr(self, '_std_get_columns'):
        rows = self._std_get_columns(schema, table, rows)
    else:
      self.get_engine(echo=echo)
      rows = self.engine_inspect.get_columns(table, schema=schema)

    rows = [get_rec(r_dict, i + 1) for i, r_dict in enumerate(rows)]

    return rows

  def get_primary_keys(self, obj, echo=False):
    "Get PK metadata for table"
    Rec = namedtuple('PKs', 'schema table pk_name column_name column_order')
    schema, table = self._split_schema_table(obj)

    def get_rec(col, pk_name, column_order):
      r_dict = {}
      r_dict['schema'] = schema
      r_dict['table'] = table
      r_dict['pk_name'] = pk_name
      r_dict['column_name'] = col
      r_dict['column_order'] = column_order
      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.primary_keys')
    if sql_tmpl:
      rows = self.select(sql_tmpl.format(table=table, schema=schema))
    else:
      self.get_engine(echo=echo)
      r_dict = self.engine_inspect.get_pk_constraint(table, schema=schema)
      rows = [
        get_rec(col, r_dict['name'], i + 1)
        for i, col in enumerate(r_dict['constrained_columns'])
      ]

    return rows

  def get_indexes(self, obj, echo=False):
    "Get indexes metadata for table"
    Rec = namedtuple(
      'Indexes', 'schema table index_name column_name column_order unique')
    schema, table = self._split_schema_table(obj)

    def get_rec(r_dict):
      r_dict['schema'] = schema
      r_dict['table'] = table
      r_dict['index_name'] = r_dict['name']
      r_dict['unique'] = str(r_dict['unique'])
      del r_dict['name']
      for i, col in enumerate(r_dict['column_names']):
        r_dict['column_name'] = col
        r_dict['column_order'] = i + 1
        yield Rec(**r_dict)

    sql_tmpl = self.template('metadata.indexes')
    if sql_tmpl:
      rows = self.select(sql_tmpl.format(table=table, schema=schema))
    else:
      self.get_engine(echo=echo)
      rows = self.engine_inspect.get_indexes(table, schema=schema)
      rows = [get_rec(r_dict) for r_dict in rows]

    return rows

  def get_ddl(self, obj, object_type=None, echo=True):
    "Get ddl for table"
    Rec = namedtuple('DDL', 'ddl')
    schema, obj = self._split_schema_table(obj)

    sql_tmpl = self.template('metadata.ddl')
    if sql_tmpl:
      rows = self.select(
        sql_tmpl.format(
          schema=schema,
          table=obj,
          obj_type=object_type,
        ))
    else:
      self.get_engine(echo=echo)
      ddl = self.engine_inspect.get_view_definition(obj, schema=schema)
      rows = [Rec(ddl)] if ddl else []

    return rows

  def analyze_fields(self, analysis, obj, fields=[], as_sql=False, **kwargs):
    """Base function for field level analysis"""
    if '.' not in obj:
      raise Exception("obj must have schema and object name in it with a '.'")
    if analysis not in self.template_dict['analysis']:
      raise Exception("'{}' not found in template for '{}'.".format(
        analysis, self.type))

    schema, table = self._split_schema_table(obj)

    # get field type
    field_rows = self.get_columns(obj)
    field_type = {r.column_name.lower(): r.type for r in field_rows}

    if not fields:
      fields = [r.column_name for r in field_rows]

    sql = ' \nunion all\n'.join([
      self.template('analysis.' + analysis).format(
        schema=schema,
        field=field,
        table=table,
        type=field_type[field.lower()],
        **kwargs) for field in fields
    ])

    return sql if as_sql else self.select(sql, analysis, echo=False)

  def analyze_tables(self, analysis, tables=[], as_sql=False, **kwargs):
    """Base function for table level analysis"""
    if analysis not in self.template_dict['analysis']:
      raise Exception("'{}' not found in template for '{}'.".format(
        analysis, self.type))

    if not tables and 'schema' in kwargs:
      # get all tables
      rows = self.get_schemas(kwargs['schema'])
      crt_obj = lambda r: struct(dict(schema=r.schema, table=r.object_name))
      objs = [crt_obj(r) for r in rows]
    else:
      crt_obj = lambda schema, table: struct(dict(schema=schema, table=table))
      objs = [crt_obj(*self._split_schema_table(t)) for t in tables]

    sql = ' \nunion all\n'.join([
      self.template('analysis.' + analysis).format(
        schema=obj.schema, table=obj.table, **kwargs) for obj in objs
    ])

    return sql if as_sql else self.select(sql, analysis, echo=False)


def get_conn(db,
             dbs=None,
             use_beeline=False,
             use_spark=False,
             echo=True,
             spark_restart=False,
             reconnect=False,
             use_jdbc=False,
             globs=globals(),
             master=None,
             spark_version=2.2,
             conn_expire_min=10,
             spark_hive=True) -> DBConn:
  global conns

  dbs = dbs if dbs else get_databases()
  profile = get_profile()
  db_dict = struct(dbs[db])

  if db_dict.type.lower() == 'hive' and spark_hive:
    db_dict.type = 'spark'

  use_jdbc = True if (use_jdbc or ('use_jdbc' in db_dict
                                   and db_dict['use_jdbc'])) else use_jdbc

  if db in conns and not reconnect:
    if (now() - conns[db].last_connect).total_seconds() / 60 < conn_expire_min:
      return conns[db]

  if use_jdbc:
    from .jdbc import JdbcConn
    conn = JdbcConn(db_dict, profile=profile)

  elif db_dict.type.lower() == 'oracle':
    from .oracle import OracleConn
    conn = OracleConn(db_dict, echo=echo)

  elif db_dict.type.lower() == 'spark':
    from .spark import SparkConn
    db_dict['restart'] = spark_restart
    db_dict['hive_enabled'] = db_dict.hive_enabled or spark_hive
    db_dict['master'] = master
    db_dict['version'] = spark_version
    conn = SparkConn(db_dict, echo=echo)

  elif db_dict.type.lower() == 'hive':
    from .hive import HiveConn, Beeline
    if use_beeline:
      conn = Beeline(db_dict, echo=echo)
    else:
      conn = HiveConn(db_dict, echo=echo)

  elif db_dict.type.lower() == 'postgresql':
    from .postgresql import PostgreSQLConn
    conn = PostgreSQLConn(db_dict, echo=echo)

  elif db_dict.type.lower() == 'sqlserver':
    from .sqlserver import SQLServerConn
    conn = SQLServerConn(db_dict, echo=echo)

  conns[db] = conn
  return conn


class SqlX:
  """
  SQL Express functions. Supports CRUD transactional operations.

  Suppose there is a table named 'cache', sqlx allows:
  
  sqlx.x('cache').insert(rows)
  sqlx.x('cache').insert_one(row)
  sqlx.x('cache').add(**kws)
  sqlx.x('cache').delete(where)
  sqlx.x('cache').update(rows, pk_fields)
  sqlx.x('cache').update_one(row, pk_cols)
  sqlx.x('cache').replace(rows, pk_fields)
  sqlx.x('cache').select(where)
  sqlx.x('cache').select_one(where)
  """

  def __init__(self, conn: DBConn, table, schema, ntRec: namedtuple):
    self.conn = conn
    self.table = table
    self.schema = schema
    self.ntRec = ntRec
    self.pk_fields = None
    self.table_obj = schema + '.' + table if schema else table

    self.insert_one = lambda row: self.insert([row])
    self.add = lambda **kws: self.insert([self.ntRec(**kws)])
    self.update_one = lambda row, pk_cols=None: self.update([row], pk_cols)
    self.update_rec=lambda pk_cols=None, **kws: self.update([make_rec(**kws)], pk_cols)
    self.replace_one = lambda row, pk_cols=None: self.replace([row], pk_cols)
    self.replace_rec=lambda pk_cols=None, **kws: self.replace([make_rec(**kws)], pk_cols)
    # self.select_one = lambda where: self.select_one(where, one=True)

  def _get_pk(self):
    if not self.pk_fields:
      pk_rows = self.conn.get_primary_keys(self.table_obj)
      self.pk_fields = [r.column_name for r in pk_rows]
    return self.pk_fields

  def insert(self, data):
    self.conn.insert(self.table_obj, data)

  def update(self, data, pk_fields=None):
    if not pk_fields:
      pk_fields = self._get_pk()
      if not pk_fields:
        raise Exception("Need Keys to perform UPDATE!")
      t_fields = [x.lower() for x in data[0]._fields]
      for f in pk_fields:
        if not f.lower() in t_fields:
          # if keys not provided, need to make sure PK values are provided in data records
          raise Exception(
            "Value of  PK field '{}' must be provided to perform UPDATE!".
            format(f))

    self.conn.update(self.table_obj, data, pk_fields, echo=False)

  def update_one(self, row, pk_cols=None):
    self.update([row], pk_cols)

  def update_rec(self, pk_cols=None, **kws):
    self.update([make_rec(**kws)], pk_cols)

  def replace(self, data, pk_fields=None):
    if not pk_fields:
      pk_fields = self._get_pk()
    self.conn.replace(self.table_obj, data, pk_fields, echo=False)

  # def replace_rec(self, pk_cols=None, **kws):
  #   # add default None?
  #   for field in self.ntRec._fields:
  #     kws[field] = kws.get(field, None)

  #   self.replace([self.ntRec(**kws)], pk_cols)

  def select(self, where='1=1', one=False, limit=None):
    rows = self.conn.select(
      "select * from {} where {}".format(self.table_obj, where),
      echo=False,
      limit=limit)
    if one: return rows[0] if rows else None
    else: return rows

  def select_one(self, where, field=None):
    row = self.select(where, one=True)
    if field and row:
      return row.__getattribute__(field)
    return row

  def delete(self, where):
    self.conn.execute("delete from {} where {}".format(self.table_obj, where))


def make_sqlx(conn, schema, tables):
  "Make sqlx lookup function for given tables"

  table_func_map: typing.Dict[str, SqlX] = {}

  for table in tables:
    ntRec = namedtuple(table, tables[table].columns.keys())
    table_func_map[table] = SqlX(conn, table, schema, ntRec)

  # return table_func_map

  def sqlx(expr) -> SqlX:
    obj = jmespath.search(expr, table_func_map)
    if not obj:
      raise Exception('sqlx: Cannot find "{}"'.format(expr))
    return obj

  return sqlx
