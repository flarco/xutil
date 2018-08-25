import datetime, csv
from xutil.database.base import DBConn
from xutil.helpers import (get_exception_message, now, log, struct,
                           is_gen_func, isnamedtupleinstance, get_dir_path)
from xutil.diskio import read_yaml, read_file, write_jsonl, read_jsonl
from collections import namedtuple


class PostgreSQLConn(DBConn):
  "PostgreSQL Connection"

  data_map = dict(
    string='VARCHAR',
    integer='integer',
    decimal='double',
    date='DATE',
    datetime='TIMESTAMP',
    timestamp='TIMESTAMP',
    text='text',
  )

  # from table pg_catalog.pg_type
  reverse_data_map = {
    16: 'integer',
    # 17:'_bytea',
    # 18:'name',
    18: 'string',
    # 19:'_name',
    20: 'integer',
    # 21:'int2vector',
    21: 'integer',
    # 22:'_int2vector',
    23: 'integer',
    # 24:'_regproc',
    25: 'string',
    # 26:'oidvector',
    # 26:'_oid',
    # 27:'_tid',
    # 28:'_xid',
    # 29:'_cid',
    # 30:'_oidvector',
    114: 'string',
    142: 'string',
    # 600:'lseg',
    # 600:'_point',
    # 600:'box',
    # 601:'_lseg',
    # 602:'_path',
    # 603:'_box',
    # 604:'_polygon',
    # 628:'_line',
    # 650:'_cidr',
    700: 'float',
    # 701:'point',
    701: 'double',
    # 701:'line',
    # 702:'_abstime',
    # 703:'_reltime',
    # 704:'_tinterval',
    # 718:'_circle',
    790: 'number',
    # 829:'_macaddr',
    # 869:'_inet',
    # 1033:'_aclitem',
    1042: 'string',
    1043: 'string',
    1082: 'datetime',
    1083: 'datetime',
    1114: 'datetime',
    1184: 'datetime',
    # 1186:'_interval',
    # 1266:'_timetz',
    # 1560:'_bit',
    # 1562:'_varbit',
    1700: 'number',
    # 1790:'_refcursor',
    # 2202:'_regprocedure',
    # 2203:'_regoper',
    # 2204:'_regoperator',
    # 2205:'_regclass',
    # 2206:'_regtype',
    # 2249:'_record',
    2275: 'string',
    2950: 'string',
    # 2970:'_txid_snapshot',
    # 3220:'_pg_lsn',
    # 3614:'_tsvector',
    # 3615:'_tsquery',
    # 3642:'_gtsvector',
    # 3734:'_regconfig',
    # 3769:'_regdictionary',
    3802: 'string',
  }

  object_type_map = {'TABLE': 'BASE TABLE', 'VIEW': 'VIEW'}

  def set_variables(self):
    self.batch_size = 50000

    self.col_name_id = 2

    self.error_msg = dict(table_not_exist='table or view does not exist', )

    self.sql_template = dict(
      drop_table='drop table if exists {}',
      insert="INSERT {options} INTO {table} ({names}) VALUES ({values})",
      create_table="create table {table}\n({col_types})",
      create_index="create index {index} on {table} ({cols})",
      insert_option='',
      schemas=
      'select schema_name from information_schema.schemata order by schema_name',
      tables=
      '''SELECT '{schema}' as schema, table_name as object_name FROM information_schema.tables WHERE UPPER(table_type) = UPPER('{obj_type}') AND UPPER(table_schema) = UPPER('{schema}') order by table_name''',
      columns='''select
      table_schema as owner, table_name, column_name, data_type,
      character_maximum_length as data_length, numeric_precision as data_precision, numeric_scale as data_scale
      from information_schema.columns
      where table_schema = '{schema}'
      and table_name =  '{table}'
      order by ordinal_position''',
      analyze='''select count(*) as tot_cnt, {fields} from {table}''',
      duplicates='''
      with t0 as (
        select
          {fields}, COUNT(1) as c
        from {table}
        group by {fields}
        having count(1) > 1
      )
      select count(1) dup_cnt from t0
      ''',
      replace='''
      INSERT INTO {table} ({names}) values ({values})
      ON CONFLICT ({pK_fields}) DO UPDATE SET {set_fields};
      ''')

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import postgresql
    return postgresql

  def create_engine(self, conn_str=None, echo=False):
    import sqlalchemy
    if not conn_str:
      conn_str = ('postgresql://{user}:{passw}@{host}:{port}/{db}'.format(
        user=self._cred.user,
        passw=self._cred.password,
        host=self._cred.host,
        port=self._cred.port,
        db=self._cred.database,
      ))

    self.engine = sqlalchemy.create_engine(conn_str, echo=echo)

    return self.engine

  def connect(self):
    "Connect / Re-Connect to Database"
    import psycopg2
    get_conn_str = lambda cred: "dbname='{}' user='{}' host='{}' port={} password='{}'".format(
      cred.database, cred.user,
      cred.host, cred.port,
      cred.password)

    cred = struct(self._cred) if isinstance(self._cred, dict) else None
    conn_str = get_conn_str(cred) if cred else self._cred
    self.connection = psycopg2.connect(conn_str)
    self.cursor = None

    self.connection.autocommit = True
    self.name = 'postgresql'
    self.username = cred.user if cred else ''

    cursor = self.get_cursor()

  def stream(self,
             sql,
             rec_name='Record',
             dtype='namedtuple',
             yield_batch=False,
             echo=True):
    "Stream Select from SQL, yield records as they come in"
    self.reconnect(min_tresh=10)
    if echo: log("Streaming SQL for '{}'.".format(rec_name))

    try:
      self.cursor.execute('{} LIMIT 0'.format(sql)
                          if 'limit ' not in sql.lower() else sql)
    except Exception as e:
      log(e)
      log(sql)
      raise e

    self.field_types = self.get_cursor_fields(as_dict=False, native_type=False)
    fields = [f for f in self.field_types]

    if dtype == 'tuple':
      make_rec = lambda row: row
    else:
      Record = namedtuple(rec_name.replace(' ', '_').replace('.', '_'), fields)
      make_rec = lambda row: Record(*row)

    autocommit = self.connection.autocommit
    self.connection.autocommit = False

    self._stream_counter = 0

    with self.connection.cursor(name='cursor') as cursor:
      cursor.itersize = self.fetch_size
      cursor.execute(sql)

      while True:
        rows = cursor.fetchmany(self.fetch_size)
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

      # self.connection.commit()
    # self.connection.autocommit = autocommit

  def insert_csv(self, table, file_path, delimiter=','):
    s_t = datetime.datetime.now()
    counter = sum(1 for line in open(file_path)) - 1
    file = open(file_path, 'r')
    file.readline()
    cursor = self.get_cursor()
    cursor.copy_from(file, table, sep=delimiter)
    self.connection.commit()

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)
    log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
      counter, table, mins, rate))

  def insert_ignore(self,
                    table,
                    data,
                    pk_fields=None,
                    commit=True,
                    echo=True,
                    temp_table=None):
    return self.replace(
      table=table,
      data=data,
      pk_fields=pk_fields,
      commit=commit,
      echo=echo,
      sql_tmpl='insert_ignore',
      temp_table=temp_table)

  def replace(self,
              table,
              data,
              pk_fields=None,
              commit=True,
              echo=True,
              sql_tmpl='replace',
              temp_table=None):
    "Upsert data into database"
    s_t = datetime.datetime.now()

    if not len(data):
      return False

    row = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(row) else 'dict'
    fields = row._fields if mode == 'namedtuple' else sorted(row.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    pk_fields_set = set(pk_fields)
    sql_tmpl = sql_tmpl + '_temp' if temp_table else sql_tmpl
    sql = self.template(sql_tmpl).format(
      table=table,
      set_fields=',\n'.join([
        '{f} = %({f})s'.format(f=f) for i, f in enumerate(fields)
        if f not in pk_fields_set
      ]),
      set_fields2=',\n'.join([
        '{f} = t2.{f}'.format(f=f) for i, f in enumerate(fields)
        if f not in pk_fields_set
      ]),
      names=',\n'.join(['{f}'.format(f=f) for f in fields]),
      pK_fields=', '.join(['{f}'.format(f=f) for f in pk_fields_set]),
      pK_fields_equal=' and '.join(
        ['t1.{f} = t2.{f}'.format(f=f) for f in pk_fields_set]),
      values=',\n'.join(['%({f})s'.format(f=f) for f in fields]),
      temp_table=temp_table,
    )

    if temp_table:
      # drop / create temp table
      self.execute('drop table if exists ' + temp_table, echo=false)
      self.execute(
        'create table {} as select * from {} where 1=0'.format(
          temp_table,
          table,
        ),
        echo=false,
      )
      self.insert(temp_table, data, echo=false)
      self.execute(sql, echo=false)
      self.execute('drop table if exists ' + temp_table, echo=false)
      counter = len(data)

    else:
      # self.connection.autocommit = False
      cursor = self.get_cursor()

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

  def insert(self, table, data, echo=True):
    headers = next(data) if is_gen_func(data) else data[0]

    mode = 'namedtuple' if isnamedtupleinstance(headers) else 'dict'
    fields = headers._fields if mode == 'namedtuple' else sorted(
      headers.keys())
    values = [i + 1
              for i in range(len(fields))] if mode == 'namedtuple' else fields

    # self.connection.autocommit = False
    cursor = self.get_cursor()
    sql = self.template('insert').format(
      table=table,
      names=', \n'.join([self._fix_f_name(f) for f in fields]),
      values=', \n'.join(['%s'] * len(values)),
    )

    i = 1

    def get_batch():
      for r, row in enumerate(data):
        yield row

    try:
      deli = '|'
      s_t = datetime.datetime.now()
      if is_gen_func(data):
        cursor.execute(sql, headers)  # insert first row
        counter = 1
      else:
        counter = 0

      temp_file_path = '{}/batch_sql.csv'.format(self.tmp_folder)
      batch_f = open(temp_file_path, 'w')
      batch_w = csv.writer(batch_f, delimiter=deli, quoting=csv.QUOTE_MINIMAL)

      for r, row in enumerate(data):
        batch_w.writerow(row)
        counter += 1
        if counter % self.batch_size == 0:
          cursor.copy_from(batch_f, table, columns=fields, sep=deli)

          batch_f = open(temp_file_path, 'w')
          batch_w = csv.writer(
            batch_f, delimiter=deli, quoting=csv.QUOTE_MINIMAL)

      batch_f.close()
      cursor.copy_from(
        open(temp_file_path, 'r'), table, columns=fields, sep=deli)
      self.connection.commit()
      os.remove(temp_file_path)

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
        counter,
        table,
        mins,
        rate,
      ))
    return counter
