# from parent folder, run 'python -m xutil.tests.helpers_test'
import unittest, os
from path import Path
from unittest.mock import patch

from xutil.helpers import *


class TestHelpers(unittest.TestCase):

  def test_profile(self):
    os.environ['PROFILE_YAML'] = get_dir_path(__file__) + '/../database/templates/profile.yaml'
    data = load_profile()
    self.assertEqual(data['databases']['PG1']['port'], 5432)

    test_prof_path = get_dir_path() + '/test.profile.yaml'
    os.environ['PROFILE_YAML'] = test_prof_path
    save_profile(data)  # Save profile data
    self.assertTrue(file_exists(test_prof_path))

    os.remove(test_prof_path)
    self.assertFalse(file_exists(test_prof_path))

  def test_state(self):
    state = State(name='test_state')
    test_data = dict(updated = now(), nested = dict(
      a=1, b=[2, 3]), array = [4, 5, 'a'])
    state.put(**test_data)
    state.save()
    state.load()

    self.assertEqual(test_data, state.data)

    state.delete()
    self.assertFalse(file_exists(state._path))





if __name__ == '__main__':
  unittest.main()