# from parent folder, run 'python -m xutil.tests.network_test'
import os
import sys
from unittest import TestCase, main

from ..network import Server

if __name__ == '__main__':
  # HOST=
  # USER=
  # PASSW=
  server = Server('test_server', HOST, USER, PASSW)
  server.ssh_connect()