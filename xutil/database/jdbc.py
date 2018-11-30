import os
from xutil.database.base import DBConn
from xutil.helpers import get_dir_path, log
from collections import namedtuple

# TODO:
# Add Metadata functions https://www.google.com/search?q=jaydebeapi+DatabaseMetaData
# getColumns
# getColumnPrivileges
# getMetaData
# getTypeInfo
# getTables
# getTableTypes

# meta = conn.connection.jconn.getMetaData()
# https://stackoverflow.com/a/21140430
# https://docs.oracle.com/javase/7/docs/api/java/sql/DatabaseMetaData.html


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
    self.meta = self.get_meta()

    # self.connection.autocommit = True
    self.name = cred.type
    self.username = cred.user if cred else ''

    cursor = self.get_cursor()

  def set_variables(self):
    if self.type == 'sqlserver':
      self.truncate_datef = lambda f: 'CONVERT(DATE, {})'.format(f)
    if self.type == 'oracle':
      self.truncate_datef = lambda f: 'trunc({})'.format(f)

  def get_meta(self):
    return self.connection.jconn.getMetaData()

  def get_catalogs(self, echo=True):
    "Get list of Catalogs."

    Rec = namedtuple('Catalogs', 'catalog')
    rs_rows = rs_to_rows(self.meta.getCatalogs())
    self._fields = Rec._fields

    rows = [Rec(r[0]) for r in rs_rows]
    return rows


  def get_schemas(self, echo=True):
    "Get list of schemas."

    Rec = namedtuple('Schemas', 'schema')
    rs_rows = rs_to_rows(self.meta.getSchemas())

    self._fields = Rec._fields

    rows = [Rec(r[0]) for r in rs_rows]
    return rows


  def get_tables(self, schema, catalog=None, echo=True):
    "Get metadata for tables."
    Rec = namedtuple('Table', 'schema table')
    self._fields = Rec._fields

    rs_rows = rs_to_rows(self.meta.getTables(catalog, schema, None, ['TABLE']))

    rows = [Rec(schema, r.TABLE_NAME) for r in rs_rows]
    return rows

  def get_views(self, schema, catalog=None, echo=True):
    "Get metadata for views."
    Rec = namedtuple('View', 'schema view')
    self._fields = Rec._fields

    rs_rows = rs_to_rows(self.meta.getTables(catalog, schema, None, ['VIEW']))

    rows = [Rec(schema, r.TABLE_NAME) for r in rs_rows]
    return rows

  def get_columns(self,
                  table_name,
                  catalog=None,
                  object_type=None,
                  echo=False,
                  include_schema_table=True,
                  native_type=True):
    "Get column metadata for table"
    # getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
    if include_schema_table:
      headers = 'schema table id column_name type nullable default autoincrement'
    else:
      headers = 'id column_name type nullable default autoincrement'

    Rec = namedtuple('Columns', headers)
    self._fields = Rec._fields
    all_rows = []

    table_names = table_name if isinstance(table_name, list) else [table_name]

    for table_name in table_names:
      schema, table = self._split_schema_table(table_name)

      def get_rec(r_dict, column_order):
        if include_schema_table:
          r_dict['schema'] = schema
          r_dict['table'] = table

        r_dict['column_name'] = r_dict['COLUMN_NAME']
        r_dict['type'] = r_dict['TYPE_NAME']
        r_dict['nullable'] = r_dict['IS_NULLABLE']
        r_dict['autoincrement'] = r_dict['IS_AUTOINCREMENT']
        r_dict['default'] = r_dict['COLUMN_DEF']
        r_dict['id'] = column_order

        for k in list(r_dict):
          if k not in headers.split():
            del r_dict[k]

        if '(' in r_dict['type']:
          r_dict['type'] = r_dict['type'].split('(')[0]

        return Rec(**r_dict)

      rs_rows = rs_to_rows(self.meta.getColumns(catalog, schema, table, None))

      all_rows += [get_rec(r._asdict(), i + 1) for i, r in enumerate(rs_rows)]
    self._fields = Rec._fields
    return all_rows

  def get_primary_keys(self, table_name, echo=False):
    "Get PK metadata for table"
    Rec = namedtuple('PKs', 'schema table pk_name column_name column_order')
    self._fields = Rec._fields
    schema, table = self._split_schema_table(table_name)

    # def get_rec(col, pk_name, column_order):
    #   r_dict = {}
    #   r_dict['schema'] = schema
    #   r_dict['table'] = table
    #   r_dict['pk_name'] = pk_name
    #   r_dict['column_name'] = col
    #   r_dict['column_order'] = column_order
    #   return Rec(**r_dict)

    # sql_tmpl = self.template('metadata.primary_keys')
    # if sql_tmpl:
    #   rows = self.select(sql_tmpl.format(table=table, schema=schema))
    # else:
    #   self.get_engine(echo=echo)
    #   r_dict = self.engine_inspect.get_pk_constraint(table, schema=schema)
    #   rows = [
    #     get_rec(col, r_dict['name'], i + 1)
    #     for i, col in enumerate(r_dict['constrained_columns'])
    #   ]
    # return rows

    # getPrimaryKeys(String catalog, String schema, String table)
    rs_rows = rs_to_rows(self.meta.getPrimaryKeys(catalog, schema, table))

    log(Exception('getPrimaryKeys not implemented!'))
    print(rs_rows)


  def get_indexes(self, table_name, echo=False):
    "Get indexes metadata for table"

    Rec = namedtuple(
      'Indexes', 'schema table index_name column_name column_order unique')
    self._fields = Rec._fields
    schema, table = self._split_schema_table(table_name)

    # def get_rec(r_dict):
    #   r_dict['schema'] = schema
    #   r_dict['table'] = table
    #   r_dict['index_name'] = r_dict['name']
    #   r_dict['unique'] = str(r_dict['unique'])
    #   del r_dict['name']
    #   for i, col in enumerate(r_dict['column_names']):
    #     r_dict['column_name'] = col
    #     r_dict['column_order'] = i + 1
    #     yield Rec(**r_dict)

    # sql_tmpl = self.template('metadata.indexes')
    # if sql_tmpl:
    #   rows = self.select(sql_tmpl.format(table=table, schema=schema))
    # else:
    #   self.get_engine(echo=echo)
    #   rows = self.engine_inspect.get_indexes(table, schema=schema)
    #   rows = [get_rec(r_dict) for r_dict in rows]

    # return rows

    # getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate)
    rs_rows = rs_to_rows(
      self.meta.getIndexInfo(catalog, schema, table, None, None))

    log(Exception('getIndexInfo not implemented!'))
    print(rs_rows)

  def get_ddl(self, table_name, object_type=None, echo=True):
    "Get ddl for table"
    Rec = namedtuple('DDL', 'ddl')
    self._fields = Rec._fields
    schema, table = self._split_schema_table(table_name)

    sql_tmpl = self.template('metadata.ddl')
    if sql_tmpl:
      rows = self.select(
        sql_tmpl.format(
          schema=schema,
          table=table,
          obj_type=object_type,
        ))
    else:
      log(Exception('get_ddl not implemented!'))

    return ['get_ddl not implemented!']


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

def rs_to_rows(rs):
  "Get Rows from JPype resultset"
  Row = None
  rows = []
  while rs.next():
    cols = [c.name for c in rs.columns]
    if not Row:
      Row = namedtuple('Row', cols)
    rows.append(Row(*rs.currentRow))
  return rows
