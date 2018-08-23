from xutil.database.base import DBConn
from xutil.helpers import struct


class SQLServerConn(DBConn):
  "SQLServer Connection"

  data_map = dict(
    string='VARCHAR2()',
    integer='NUMBER()',
    decimal='NUMBER()',
    date='DATE',
    datetime='DATE',
    text='CLOB',
  )

  truncate_datef = lambda self, f: 'CONVERT(DATE, {})'.format(f)

  def set_variables(self):
    self.batch_size = 50000

    self.error_msg = dict(table_not_exist='table or view does not exist', )

    self.sql_template = dict(
      drop_table='drop table {} cascade constraints purge',
      insert="INSERT {options} INTO {table} ({names}) VALUES ({values})",
      create_table="create table {table}\n({col_types})",
      create_index="create index {index} on {table} ({cols})",
      insert_option='',
      columns='''select
      owner, table_name, column_name, data_type,
      data_length, data_precision, data_scale
      from all_tab_columns
      where owner = '{owner}'
      and table_name =  '{table}'
      order by column_id''',
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
      replace='''merge into {table} tgt
      USING (SELECT {name_values}
              FROM dual) src
      ON ({src_tgt_condition})
      WHEN MATCHED THEN
        UPDATE SET {set_fields}
      WHEN NOT MATCHED THEN
          insert ({names}) values ({values})
      ''')

  def connect(self):
    "Connect / Re-Connect to Database"
    import pyodbc

    cred = struct(self._cred) if isinstance(self._cred, dict) else None

    self.odbc_string = 'DRIVER={odbc_driver};SERVER={host};DATABASE={database};UID={user};PWD={password}'.format(
      odbc_driver=cred.odbc_driver,
      host=cred.host,
      database=cred.database,
      user=cred.user,
      password=cred.password,
    )
    self.connection = pyodbc.connect(self.odbc_string)

    self.cursor = None

    # self.connection.autocommit = True
    self.name = 'sqlserver'
    self.username = cred.user if cred else ''

    cursor = self.get_cursor()

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import mssql
    return mssql

  def create_engine(self, conn_str=None, echo=False):
    import sqlalchemy, urllib

    conn_str = ('mssql+pyodbc:///?odbc_connect={}'.format(
      urllib.parse.quote_plus(self.odbc_string)))

    self.engine = sqlalchemy.create_engine(conn_str, echo=echo)

    return self.engine
