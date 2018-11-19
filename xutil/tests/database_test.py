# from parent folder, run 'python -m xutil.tests.database_test'
# coverage run  -m xutil.tests.database_test
# coverage report
# coverage html

import sys
from unittest import TestCase, main
from copy import deepcopy
from xutil.database.base import get_conn
from xutil.helpers import get_profile, get_databases, elog
from xutil.diskio import read_yaml

orig_stdout = sys.stdout
orig_stderr = sys.stderr


def test_join_match():
  t1 = 'bank.mint_categories'
  t2 = 'bank.mint_categories'
  t1_field = 'category, sub_category'
  t2_field = 'category, sub_category'
  conn = get_conn('PG_XENIAL')
  rows = conn.analyze_join_match(t1, t2, t1_field, t2_field, as_sql=False)
  print(rows)


class DatabaseTest(TestCase):
  debug = True


def make_test_functions(db_name, schema, obj, pre_sql=None):
  def setUp(self):
    self.conn = get_conn(db_name, echo=False)
    if pre_sql:
      self.conn.execute(pre_sql)

  def test_get_schemas(self):
    rows = self.conn.get_schemas()
    self.assertGreater(len(rows), 0, msg='Failed to obtain schemas.')

  def test_get_objects(self):
    rows = self.conn.get_objects(schema=schema)
    self.assertGreater(
      len(rows), 0, msg='Failed to obtain objects for "{}".'.format(schema))

  def test_get_columns(self):
    obj_combo = schema + '.' + obj
    rows = self.conn.get_columns(obj=obj_combo)
    self.assertGreater(
      len(rows), 0, msg='Failed to obtain columns for "{}".'.format(obj_combo))

  def test_get_primary_keys(self):
    obj_combo = schema + '.' + obj
    rows = self.conn.get_primary_keys(obj=obj_combo)
    self.assertGreater(
      len(rows), 0, msg='Failed to obtain PKs for "{}".'.format(obj_combo))

  def test_get_ddl(self):
    obj_combo = schema + '.' + obj
    rows = self.conn.get_ddl(obj=obj_combo)
    self.assertGreater(
      len(rows), 0, msg='Failed to obtain ddl for "{}".'.format(obj_combo))

  def test_analyze_fields(self):
    obj_combo = schema + '.' + obj
    for analyis_name in self.conn.template_dict['analysis']:
      if not (analyis_name.startswith('field')
              or analyis_name.startswith('distro')):
        continue

      kwargs = {}
      if analyis_name in ('distro_field_group', 'field_stat_group'):
        kwargs['group_expr'] = '1'

      if analyis_name in ('distro_field', 'distro_field_group'):
        field_rows = self.conn.get_columns(obj=obj_combo)
        kwargs['fields'] = [field_rows[0].column_name]

      if analyis_name in ('distro_field_date', 'distro_field_date_wide'):
        field_rows = self.conn.get_columns(obj=obj_combo)
        date_fields = [
          f for f in field_rows
          if 'date' in f.type.lower() or 'time' in f.type.lower()
        ]
        if not date_fields:
          continue
        else:
          field = date_fields[0].column_name

        kwargs['fields'] = [field]
        if analyis_name in ('distro_field_date_wide'):
          kwargs['fields'] = [f.column_name for f in field_rows]
          kwargs['date_field'] = field
          cnt_tmpl = 'sum(case when {field} is null then 0 else 1 end) as {field}_cnt'
          kwargs['cnt_fields_sql'] = ', '.join(
            [cnt_tmpl.format(field=f) for f in kwargs['fields']]),

      rows = self.conn.analyze_fields(analyis_name, obj=obj_combo, **kwargs)
      self.assertGreater(
        len(rows),
        0,
        msg='Failed to run analysis "{}" for "{}".'.format(
          analyis_name, obj_combo))

  def test_analyze_tables(self):
    obj_combo = schema + '.' + obj
    for analyis_name in self.conn.template_dict['analysis']:
      err_msg = Exception('Failed to run analysis "{}" for "{}".'.format(
        analyis_name, obj_combo))
      if not analyis_name.startswith('table'):
        continue

      kwargs = {}
      if analyis_name in ('table_join_match'):
        field_rows = self.conn.get_columns(obj=obj_combo)
        field = field_rows[0].column_name
        kwargs['t1'] = obj_combo
        kwargs['t2'] = obj_combo
        kwargs['t1_field'] = 't1.{0}'.format(field)
        kwargs['t1_fields1'] = field
        kwargs['t1_filter'] = ''
        kwargs['t2_field'] = 't2.{0}'.format(field)
        kwargs['t2_fields1'] = field
        kwargs['t2_filter'] = ''
        kwargs['conds'] = 't1.{0} = t2.{0}'.format(field)

      try:
        rows = self.conn.analyze_tables(
          analyis_name, tables=[obj_combo], **kwargs)
      except Exception as E:
        log(err_msg)
        raise E
      self.assertGreater(len(rows), 0, msg=err_msg)

  return dict(
    setUp=setUp,
    test_get_schemas=test_get_schemas,
    test_get_objects=test_get_objects,
    test_get_columns=test_get_columns,
    # test_get_primary_keys=test_get_primary_keys,
    test_get_ddl=test_get_ddl,
    test_analyze_fields=test_analyze_fields,
    test_analyze_tables=test_analyze_tables,
  )


def test_lineage():
  from xutil.database.base import get_sql_sources
  from xutil.diskio import read_file
  from pprint import pprint

  sql = read_file(r'C:\__\Temp\test.sql')
  sources = get_sql_sources(sql)
  pprint(sources)


if __name__ == '__main__':
  test_lineage()
  sys.exit(0)

  dbs = get_databases()
  for db_name in dbs['TESTS']:
    schema, obj = dbs['TESTS'][db_name]['object'].split('.')
    pre_sql = dbs['TESTS'][db_name]['pre_sql'] if 'pre_sql' in dbs['TESTS'][
      db_name] else None
    class_name = 'DbTest_' + db_name
    globals()[class_name] = type(
      class_name, (DatabaseTest, ),
      make_test_functions(db_name, schema, obj, pre_sql))

  main(verbosity=2)
