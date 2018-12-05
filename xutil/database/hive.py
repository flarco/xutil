from .base import DBConn
import sys, os
from xutil.helpers import (get_exception_message, now, log, struct, log)
from xutil.diskio import read_yaml, read_csvD, write_jsonl, read_jsonl


class HiveConn(DBConn):
  "Hive Connection"

  data_map = dict(
    string='VARCHAR()',
    integer='NUMBER()',
    decimal='NUMBER()',
    date='DATE',
    datetime='DATE',
    text='CLOB',
  )

  reverse_data_map = dict(
    INT_TYPE='integer',
    TIMESTAMP_TYPE='datetime',
    STRING_TYPE='string',
  )

  def connect(self):
    "Connect / Re-Connect to Database"
    c = self._cred

    # import pyhs2
    # self.connection = pyhs2.connect(
    #   host=c.host,
    #   port=c.port,
    #   authMechanism="KERBEROS",
    #   password=c.password,
    #   user=c.user)
    # self.connection.autocommit = True

    from impala.dbapi import connect
    self.connection = connect(
      host=c.host,
      port=c.port,
      **c.kwargs)
    self.cursor = None

    self.name = "Hive"
    self.username = c.user

  def set_variables(self):

    self.col_name_id = 0

    self.error_msg = dict(table_not_exist='table or view does not exist', )

    self.sql_template = dict(
      drop_table='drop table {} cascade constraints',
      insert="INSERT {options} INTO {table} ({names}) VALUES ({values})",
      create_table="create table if not exists {table} ({col_types}) {options}",
      create_index="create index {index} on {table} ({cols})",
      analyze='''select count(*) as tot_cnt, {fields} from {table}''',
      columns='''describe {schema}.{table}''',
      tables='''show tables in {schema}''',
      schemas='''show databases''',
      ddl='''show create table {schema}.{table}''',
      insert_option='',
    )

    self.temp_path = self.tmp_folder

  def get_engine(self, echo=False):
    "Could implement PyHive?"
    return None


  def get_cursor_metadata(self, schema, table, sample_row=None):
    "Get current cursor column metadata"
    samples = sample_row._asdict() if sample_row else {
      fix_field(f['columnName']): None
      for f in self.cursor.getSchema()
    }
    data = [
      self.nt_cursor_metadata(
        schema,
        table,
        fix_field(f['columnName']),
        f['type'],
        n + 1,
        samples[fix_field(f['columnName'])],
        schema + '.' + table,
        schema + '.' + table + '.' + fix_field(f['columnName']),
      ) for n, f in enumerate(self.cursor.getSchema())
    ]

    return data

  def _do_execute(self, sql, cursor=None):
    super(HiveConn, self)._do_execute(sql, cursor=cursor)
    cleanse_f = lambda f: f.split('.')[-1] if '.' in f else f
    self._fields = [cleanse_f(f) for f in self._fields]


  # def get_cursor_fields(self, cursor=None):
  #   "Get fields of active Select cursor"
  #   cursor = cursor if cursor else self.cursor
  #   fields = [fix_field(f['columnName']) for f in cursor.getSchema()]
  #   return fields


  # def select(self,
  #            sql,
  #            nt_name='Record',
  #            dtype='namedtuple',
  #            limit=None,
  #            date_fields_=[],
  #            echo=True,
  #            date_conv=True,
  #            date_conv_str=False,
  #            log=log):
  #   "Select from SQL, return list of namedtuples"
  #   data = super(HiveConn, self).select(
  #     sql, nt_name, dtype='dataframe', echo=echo)

  #   # Hive returns date fields as text. Not useful.
  #   # This is to convert from text to date using ever-efficient dataframes
  #   fields = [f['columnName'] for f in self.cursor.getSchema()]

  #   date_fields = [fix_field(f['columnName']) for f in self.cursor.getSchema() \
  #                  if 'DATE' in f['type'] or 'TIME' in f['type']]

  #   if date_conv_str:
  #     date_fields = [fix_field(f['columnName']) for f in self.cursor.getSchema() \
  #                    if 'DATE' in f['type'] or 'TIME' in f['type'] or 'date' in f['columnName'].lower() \
  #                    or f['columnName'].lower().endswith('_dt')]

  #   date_fields = date_fields + [f for f in date_fields_ if f in fields]

  #   if date_fields and date_conv:
  #     if echo: log('>> Attempting to convert date strings to date types.')
  #     str_io = StringIO()  # text in memory
  #     data.to_csv(str_io, index=False)
  #     str_io.seek(0)  # go to beginning of virtual file
  #     try:
  #       data = read_csvD(str_io, date_cols=list(set(date_fields)))
  #       if echo: log('>> Done Converting')
  #     except Exception as e:
  #       message = get_exception_message().lower()
  #       log('ERROR Converting: ' + get_exception_message())

  #   if dtype == 'namedtuple':
  #     data = df_to_list(data)

  #   return data

  @staticmethod
  def execute_cli(sql,
                  output_path=None,
                  delimiter=',',
                  ret_data=True,
                  date_cols=[],
                  use_beeline=False,
                  ret_df=False,
                  echo=True):
    "Execute using command line"
    deli = '\\' + delimiter if delimiter in ['|'] else delimiter

    cmd_tmpl = self.variables[
      'beeline_cmd'] if use_beeline else self.variables['hive_cmd']

    if output_path: cmd_tmpl = '{} > {}'.format(cmd_tmpl, output_path)
    command = cmd_tmpl.format(sql=sql, deli=deli)
    # print(command)
    s_t = datetime.datetime.now()
    process = subprocess.Popen(
      command,
      shell=True,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
    )
    # print(command)

    pid = process.pid

    if echo: log("Running Hive query. PID is {}".format(pid))

    data = []
    Rec = None

    try:
      if not output_path and ret_data:
        v_file_text = b''.join(process.stdout).decode('utf8')
        # print(v_file_text)
        v_file = StringIO(v_file_text)
        reader = csv.reader(v_file, delimiter=',')
        Rec = namedtuple('Rec', [
          h.replace(' ', '_').split('.')[1] if '.' in h else h
          for h in [str(v) for v in next(reader)]
        ])
        data = [row for i, row in enumerate(imap(Rec._make, reader))]

      process.wait()
      stderr = [line.rstrip().decode('utf8') for line in process.stderr]
      # print(stderr)
      if 'Error: ' in '\n'.join(stderr[-2:]) and use_beeline:
        print('\n'.join(stderr[-2:]))
        print(stderr)
        return []

    except KeyboardInterrupt:
      process.terminate()
      log("PID {} terminated!".format(pid))
      return []

    counter = len(data)
    if output_path:
      if ret_data:
        data = read_csvD(
          output_path, delimiter=delimiter, echo=False, date_cols=date_cols)
        if ret_df:
          fix_col = lambda c: c.replace(' ', '_').split('.')[-1] if '.' in c else c.replace(' ', '_')
          data.rename(
            columns={c: fix_col(c)
                     for c in data.columns}, inplace=True)
        else:
          data = df_to_list(data)

        counter = len(data)
      else:
        if stderr and 'rows selected' in stderr[-3] and use_beeline:
          counter = int(stderr[-3].split()[0].replace(',', ''))
        elif stderr and 'Fetched' in stderr[-1]:
          counter = int(stderr[-1].split()[-2].replace(',', ''))

    output, error = process.communicate()
    returncode = process.returncode

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)

    if echo:
      log("Hive query finished! Got {} records in {} mins [{} r/s].".format(
        counter, mins, rate))

    return data

  def _std_get_columns(self, schema, table, rows):
    "Standardize for get_columns"
    make_rec = lambda r, i: dict(name=r.col_name, type=r.data_type, schema=schema, table=table, nullable=None, default=None, autoincrement=None, id=i)
    data = [make_rec(r, i) for i, r in enumerate(rows)]
    return data

  def _std_get_tables(self, schema, rows):
    "Standardize for get_tables"
    data = [r.tableName for r in rows if not r.isTemporary]
    return data


class Beeline(HiveConn):
  "Any JDBC Connection String"

  cmd_tmpl = """{cmd} > {path}"""

  def get_columns(self, table, names_only=False, echo=False):
    "Get column metadata for table"
    owner, table = table.split('.') if '.' in table else (self.username, table)
    sql = self.sql_template['columns'].format(
      owner=owner,
      table=table,
    )
    Rec = namedtuple('Rec', 'column_name data_type')
    if names_only:
      return [r[0] for r in self.select(sql, table + ' columns', echo=echo)]
    else:
      return [
        Rec(r[0], r[1])
        for r in self.select(sql, table + ' columns', echo=echo)
      ]

  def convert_to_parquet(self, table, order_by=[]):
    "Convert table to parquet format"
    sql = '''use {db};
    drop table if exists {table}z;
    create table {table}z stored as parquet as
    select * from {table} {ord};
    drop table {table};
    '''.format(
      db=table.split('.')[0],
      table=table,
      ord='order by ' + ', '.join(order_by) if order_by else '',
    )

    # get table path
    data = self.select('show create table ' + table, echo=False)
    for i, r in enumerate(data):
      if data[i][0].strip() == 'LOCATION':
        table_hdfs_path = data[i + 1][0].strip().replace("'", '')
        log('HDFS Path: ' + table_hdfs_path)
        break

    # delete z folder, create z temp table
    os.system('hdfs dfs -rm -r -f -skipTrash {}z'.format(table_hdfs_path))
    self.execute(sql, echo=False)

    # delete orignal folder, create final table
    # When renaming, error keeps occuring
    os.system('hdfs dfs -rm -r -f -skipTrash {}'.format(table_hdfs_path))
    sql = '''use {db}; alter table {table}z rename to {table}'''.format(
      db=table.split('.')[0],
      table=table,
    )
    self.execute(sql, echo=False)

  def select_to_csv(self,
                    sql,
                    file_path=None,
                    nt_name='Record',
                    echo=True,
                    echo2=True):
    "Select from SQL, write to csv"

    t_suff = datetime.datetime.now().strftime('%Y%m%d-%H%M')
    pwd_path = '{}/p_{}_{}'.format(self.temp_path, self._cred.name, t_suff)
    file_path = '{}/data_{}.csv'.format(self.temp_path,
                                        t_suff) if not file_path else file_path

    os.system('mkdir -p ' + self.temp_path)
    # write_to_file(self._cred.password, pwd_path)

    command = self.cmd_tmpl.format(
      cmd=self.variables['beeline_cmd'].format(sql),
      path=file_path,
    )

    s_t = datetime.datetime.now()
    process = subprocess.Popen(
      command,
      shell=True,
      stdout=subprocess.PIPE,
      stderr=subprocess.PIPE,
    )

    pid = process.pid

    if echo: log("Running Beeline query. PID is {}.".format(pid))

    try:
      process.wait()
      stderr = [line.rstrip().decode('utf8') for line in process.stderr]
      if 'Error: ' in '\n'.join(stderr):
        print('\n'.join(stderr[-2:]))
        raise Exception('Hive Exception: ' + '\n'.join(stderr))

        print(stderr)
        print(command)
        return []

    except KeyboardInterrupt:
      process.terminate()
      log("PID {} terminated!".format(pid))
      return []
    finally:
      os.system('rm -f ' + pwd_path)

    output, error = process.communicate()
    returncode = process.returncode

    counter = int(stderr[-3].split()[0].replace(
      ',', '')) if stderr and 'rows selected' in stderr[-3] else 0

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)

    if echo and echo2:
      log("Wrote {} records to {} in {} mins [{} r/s].".format(
        counter, file_path, mins, rate))

  def execute(self, sql, echo=True, echo2=True):
    "Excecute SQL"
    s_t = datetime.datetime.now()
    t_suff = datetime.datetime.now().strftime('%Y%m%d-%H%M')
    file_path = '{}/data_{}.csv'.format(self.temp_path, t_suff)
    self.select_to_csv(sql, file_path, echo=echo, echo2=False)
    os.system('rm -f ' + file_path)

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)

    if echo:
      log("Beeline query finished in {} secs / {} mins.".format(secs, mins))

  def select(self,
             sql,
             nt_name='Record',
             dtype='namedtuple',
             limit=None,
             date_cols=[],
             echo=True,
             log=log):
    "Select from SQL, return list of namedtuples"
    t_suff = datetime.datetime.now().strftime('%Y%m%d-%H%M')
    file_path = '{}/data_{}.csv'.format(self.temp_path, t_suff)

    # Run Query stdout to file
    s_t = datetime.datetime.now()
    self.select_to_csv(sql, file_path, nt_name=nt_name, echo=echo, echo2=False)

    data = read_csvD(file_path, echo=False, date_cols=date_cols)
    os.system('rm -f ' + file_path)

    if dtype == 'dataframe':
      fix_col = lambda c: c.replace(' ', '_').split('.')[-1] if '.' in c else c.replace(' ', '_')
      data.rename(columns={c: fix_col(c) for c in data.columns}, inplace=True)
    else:
      data = df_to_list(data)

    counter = len(data)
    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)

    if echo:
      log("Beeline SELECT query finished! Got {} records in {} mins [{} r/s].".
          format(counter, mins, rate))

    return data

  def _concat_fields(self, fields, as_text=False):
    if as_text:
      fields = ['cast({} as string)'.format(f) for f in fields]
    return 'concat({})'.format(', '.join(fields))

  def get_tables(self, schema, echo=True):
    "Get metadata for tables."
    Rec = namedtuple('Table', 'schema table')

    def get_rec(table):
      r_dict = dict(schema=schema, table=table)
      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.tables')
    tables = [r[1] for r in self.select(sql_tmpl.format(schema=schema))]
    rows = [get_rec(t) for t in sorted(tables)]
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
      r_dict['column_name'] = r_dict['col_name']
      r_dict['type'] = r_dict['data_type']
      r_dict['column_order'] = column_order
      r_dict['nullable'] = None
      r_dict['default'] = None
      r_dict['autoincrement'] = None

      del r_dict['col_name']
      del r_dict['data_type']
      if 'attrs' in r_dict: del r_dict['attrs']
      return Rec(**r_dict)

    sql_tmpl = self.template('metadata.columns')
    rows = self.select(sql_tmpl.format(table=table, schema=schema))
    rows = [get_rec(r.asDict(), i + 1) for i, r in enumerate(rows)]

    return rows