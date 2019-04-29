# from parent folder, run 'python -m xutil.tests.helpers_test'
import unittest, os
from path import Path
from unittest.mock import patch

from xutil.helpers import *
from xutil.database.base import get_conn

import docker  # https://github.com/docker/docker-py

from collections import namedtuple


class BaseDBTest(unittest.TestCase):
  timeout = 40

  @classmethod
  def setUpClass(cls):
    cls.client = docker.from_env()
    cls.container = cls.client.containers.run(**cls.cntnr_params)
    log('-Waiting for {} to start...'.format(cls.db_name))

    st = now()
    os.environ['PROFILE_YAML'] = get_dir_path(
      __file__) + '/../database/templates/profile.yaml'
    cls.conn = None

    while 1:
      time.sleep(1)
      try:
        cls.conn = get_conn(cls.db_name, echo=False)
        break
      except Exception as E:
        if tdelta_seconds(now(), st) > cls.timeout:
          cls.container.kill()
          raise E

  @classmethod
  def tearDownClass(cls):
    cls.conn.connection.close()
    cls.container.kill()


class TestPostGres(BaseDBTest):
  db_name = 'PG_TEST'
  cntnr_params = dict(
    image="postgres:9.6",
    detach=True,
    remove=True,
    ports={5432: 35432},
    environment={
      'POSTGRES_DB': "test_db",
      'POSTGRES_USER': "user",
      'POSTGRES_PASSWORD': "password",
    },
  )



  def test_load_data(self):

    table_name = 'public.test_table'
    Row = namedtuple('Row', 'id name state time comment timestamp')
    test_data = []
    for i in range(51):
      test_data += [
        Row('a'+str(i), 'Fritz', 'Florida', epoch(), 'some\ncomment\nwith new line', now()),
        Row('b'+str(i), 'James', 'California', epoch(),
          '''comment\twith\n comma, 'tab' and "quotes" ''',
            now())
      ]
      
    field_types = dict(
      id=('string', 0, 15),
      name=('string', 0, 100),
      state=('string', 0, 100),
      time=('integer', 0, epoch()),
      comment=('string', 0, 100),
      timestamp=('timestamp', 0, 100),
    )
    self.conn.batch_size = 100
    self.conn.create_table(table_name, field_types)
    self.conn.execute('ALTER TABLE test_table ADD PRIMARY KEY (id)')
    count = self.conn.insert(table_name, test_data)
    self.assertEqual(count, len(test_data))

    fields, rows = self.conn.execute(
      'select * from ' + table_name, dtype='tuple')
    self.assertEqual([tuple(r) for r in test_data], rows)
    self.assertEqual(fields, list(Row._fields))


    # Test analysis
    data = self.conn.analyze_fields('field_chars', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('field_stat_deep', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('field_stat_deep', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('distro_field', table_name, fields=['state'])
    self.assertEqual(2, len(data))

    data = self.conn.analyze_fields('distro_field_date', table_name, fields=['timestamp'])
    self.assertEqual(1, len(data))

    # Test replace
    test_data[0] = Row(test_data[0][0], 'Emma', 'Florida', epoch(), 'some\ncomment\nwith new line', now())
    count = self.conn.replace('test_table', test_data, pk_fields=['id'])
    fields, rows = self.conn.execute(
      'select * from test_table', dtype='tuple')
    self.assertEqual([tuple(r) for r in test_data], rows)


class TestOracle(BaseDBTest):

  db_name = 'ORCL_TEST'
  cntnr_params = dict(
    image="flarco/oracle-xe-11g:v1",
    detach=True,
    remove=True,
    ports={1521: 31521},
  )



  def test_load_data(self):

    table_name = 'system.test_table'
    Row = namedtuple('Row', 'id_ name_ state_ time_ comment_ timestamp_')
    test_data = []
    for i in range(51):
      test_data += [
        Row('a'+str(i), 'Fritz', 'Florida', epoch(), 'some\ncomment\nwith new line', now().replace(microsecond=0)),
        Row('b'+str(i), 'James', 'California', epoch(),
          '''comment\twith\n comma, 'tab' and "quotes" ''',
            now().replace(microsecond=0))
      ]
      
    field_types = dict(
      id_=('string', 15, None),
      name_=('string', 100, None),
      state_=('string', 100, None),
      time_=('integer', 15, None),
      comment_=('string', 100, None),
      timestamp_=('timestamp', None, None),
    )
    self.conn.batch_size = 100
    self.conn.create_table(table_name, field_types)
    count = self.conn.insert(table_name, test_data)
    self.assertEqual(count, len(test_data))

    fields, rows = self.conn.execute(
      'select * from '+table_name, dtype='tuple')
    self.assertEqual([tuple(r) for r in test_data], rows)
    self.assertEqual(fields, list(Row._fields))


    # Test analysis
    data = self.conn.analyze_fields('field_chars', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('field_stat_deep', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('field_stat_deep', table_name)
    self.assertEqual(fields, [r.field for r in data])

    data = self.conn.analyze_fields('distro_field', table_name, fields=['state_'])
    self.assertEqual(2, len(data))

    data = self.conn.analyze_fields('distro_field_date', table_name, fields=['timestamp_'])
    self.assertEqual(1, len(data))


if __name__ == '__main__':
  unittest.main()