# from parent folder, run 'python -m xutil.tests.helpers_test'
import unittest, os
from path import Path
from unittest.mock import patch

from xutil.helpers import file_exists, epoch, get_dir_path
from xutil.diskio import *

from collections import namedtuple


class TestDiskIO(unittest.TestCase):

  def test_yaml(self):
    # test write
    test_path = get_dir_path() + '/test.yaml'
    test_data = {
      'name': 'Fritz',
      'time': epoch(),
      'address': {
        'state': 'Florida'
      }
    }
    write_yaml(test_path, test_data)
    self.assertTrue(file_exists(test_path))

    # test read
    data = read_yaml(test_path)
    self.assertEqual(data, test_data)

    # delete test file
    os.remove(test_path)


  def test_file(self):
    test_data = '''hello\nthis comment\twith\n comma, 'tab' and "quotes"'''
    test_path = get_dir_path() + '/test.txt'

    # test write
    write_file(test_path, test_data)
    self.assertTrue(file_exists(test_path))

    # test read
    data = read_file(test_path)
    self.assertEqual(data, test_data)
    
    # delete test file
    os.remove(test_path)

  def test_csv(self):
    Row = namedtuple('Row', 'name state time comment')
    test_path = get_dir_path() + '/test.csv'
    test_data = [
      Row('Fritz', 'Florida', str(epoch()), 'some\ncomment\nwith new line'),
      Row('James', 'California', str(epoch()), '''comment\twith\n comma, 'tab' and "quotes" ''')
    ]

    # test write
    write_csv(test_path, Row._fields, test_data)
    self.assertTrue(file_exists(test_path))

    # test read
    data = read_csv(test_path)
    self.assertEqual(data, test_data)

    # delete test file
    os.remove(test_path)


    # test write stream
    def stream():
      for row in test_data:
        yield row

    write_csvs(test_path, stream())
    self.assertTrue(file_exists(test_path))

    # test read stream
    data = list(read_csvS(test_path))
    self.assertEqual(data, list(test_data))

    # test read dataframe
    df = read_csvD(test_path)
    self.assertEqual(list(df.columns), list(Row._fields))
    self.assertEqual(len(df), len(test_data))
    self.assertEqual(df['comment'].loc[1], test_data[1].comment)

    # delete test file
    os.remove(test_path)


if __name__ == '__main__':
  unittest.main()