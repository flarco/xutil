# from parent folder, run 'python -m xutil.tests.web_test'

import sys, time
from unittest import TestCase, main

from xutil.web import create_sio_client
from multiprocessing import Process


class WebTest(TestCase):
  debug = True


def create_test_app(port):
  from xutil.web import WebApp
  app = WebApp(name='AppTest', port=port)

  @app.route('/')
  def index():
    """Serve the client-side application."""
    return 'Hi!'

  @app.on('connect')
  def connect(sid, environ):
    print('connect ', sid)

  @app.on('message')
  def message(sid, data):
    print('message ', data)
    return {'x': 'OK'}

  @app.on('disconnect')
  def disconnect(sid):
    print('disconnect ', sid)

  app.run()


def make_test_functions(port):
  def setUp(self):
    self.app_proc = Process(target=create_test_app, args=(port, ))
    self.app_proc.start()
    # time.sleep(1)

  def tearDown(self):
    self.app_proc.terminate()

  def test_web_app(self):
    import requests
    url = 'http://localhost:{}/'.format(port)
    resp = requests.get(url)
    self.assertEqual(resp.text, 'Hi!')

  def test_sio(self):
    func_map = dict(
      message_response=lambda data: self.assertEqual(data['x'], 'OK'))
    sio_client = create_sio_client('localhost', port, func_map)
    sio_client.emit('message', 'HEY')

  return dict(
    # test_web_app=test_web_app,
    # test_sio=test_sio,
  )


if __name__ == '__main__':
  class_name = 'WebTest'
  globals()[class_name] = type(
    class_name, (WebTest, ), make_test_functions(port=5889))

  main(verbosity=2)