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
        cls.conn = get_conn(cls.db_name)
        break
      except Exception as E:
        if tdelta_seconds(now(), st) > cls.timeout:
          cls.container.kill()
          raise E

  @classmethod
  def tearDownClass(cls):
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

    Row = namedtuple('Row', 'name state time comment')
    test_data = [
      Row('Fritz', 'Florida', epoch(), 'some\ncomment\nwith new line'),
      Row('James', 'California', epoch(),
          '''comment\twith\n comma, 'tab' and "quotes" ''')
    ]
    field_types = dict(
      name=('string', 0, 100),
      state=('string', 0, 100),
      time=('integer', 0, epoch()),
      comment=('string', 0, 100),
    )
    self.conn.create_table('test_table', field_types)
    count = self.conn.insert('test_table', test_data)
    self.assertEqual(count, len(test_data))

    fields, rows = self.conn.execute(
      'select * from test_table', dtype='tuple')
    self.assertEqual([tuple(r) for r in test_data], rows)
    self.assertEqual(fields, list(Row._fields))


class TestOracle(BaseDBTest):

  db_name = 'ORCL_TEST'
  cntnr_params = dict(
    image="flarco/oracle-xe-11g:v1",
    detach=True,
    remove=True,
    ports={1521: 31521},
  )



  def test_load_data(self):

    Row = namedtuple('Row', 'name_ state_ time_ comment_')
    test_data = [
      Row('Fritz', 'Florida', epoch(), 'some\ncomment\nwith new line'),
      Row('James', 'California', epoch(),
          '''comment\twith\n comma, 'tab' and "quotes" ''')
    ]
    field_types = dict(
      name_=('string', 500, None),
      state_=('string', 500, None),
      time_=('integer', 15, None),
      comment_=('string', 500, None),
    )
    self.conn.create_table('test_table', field_types)
    count = self.conn.insert('test_table', test_data)
    self.assertEqual(count, len(test_data))

    fields, rows = self.conn.execute(
      'select * from test_table', dtype='tuple')
    self.assertEqual([tuple(r) for r in test_data], rows)
    self.assertEqual(fields, list(Row._fields))


if __name__ == '__main__':
  unittest.main()