# from parent folder, run 'python -m xutil.tests.helpers_test'

from ..helpers import *

if __name__ == '__main__':
  log('Hey Normal')
  slog('Hey Success')
  elog('Hey Error')