# Web Lib
'''
HTML
eMailing
Telegram
Web Hooks
'''
import os

## HTML ####################

from xutil.helpers import (slog, elog, log)


def generate_rmd_html(rmd_file,
                      output_file='NULL',
                      output_dir='NULL',
                      var_data={},
                      print_output=False,
                      serve=False):
  "rmd_file is the file path to the Rmd file. HTML document will be generated in same folder as Rmd file."
  import os, subprocess
  pandoc_path = os.getenv('R_PANDOC_HOME')
  os.environ["PATH"] = '{}:{}'.format(pandoc_path, os.environ[
    "PATH"]) if pandoc_path not in os.environ["PATH"] else os.environ["PATH"]

  # Apply key-value variables to Rmd file
  if len(var_data):
    if output_file == 'NULL':
      raise Exception(
        'Parameter "output_file" must be specified when providing "var_data" values!'
      )
    if output_dir == 'NULL':
      raise Exception(
        'Parameter "output_dir" must be specified when providing "var_data" values!'
      )
    if not Path(rmd_file).exists():
      raise Exception('File "{}" does not exists'.format(rmd_file))

    rmd_new = output_dir + '/' + output_file.replace('.html', '') + '.Rmd'
    output_file = output_file if '.' in output_file else output_file + '.html'

    if rmd_new == rmd_file:
      raise Exception(
        'Parameter name of "output_file" must be different from  "rmd_file"!')

    os.system("cp -f {} {}".format(rmd_file, rmd_new))  # backup copy
    rmd_text = read_file(rmd_new)
    rmd_text = str_format(rmd_text, var_data, '$')
    write_to_file(rmd_new, rmd_text)
    rmd_file = rmd_new

  add_quotes = lambda t: '"' + t + '"' if t != 'NULL' else t
  cmd = """R -e 'library(rmarkdown); rmarkdown::render("{rmd_file}", "html_document", {output_file}, {output_dir})'""".format(
    rmd_file=rmd_file,
    output_file=add_quotes(output_file),
    output_dir=add_quotes(output_dir),
  )
  out_text = subprocess.getoutput(cmd)

  success = 'Output created: ' in out_text
  if print_output or not success: print(out_text)

  html_file = [
    line for line in out_text.splitlines() if 'Output created: ' in line
  ][0].split(': ')[-1] if success else None

  if serve: serve_http_root(port=5567, http_path=html_file)

  return html_file


def extract_text_from_html(html_source):
  from bs4 import BeautifulSoup

  def visible(element):
    if element.parent.name in [
        'style', 'script', '[document]', 'head', 'title'
    ]:
      return False
    elif re.match('<!--.*-->', str(element)):
      return False
    return True

  soup = BeautifulSoup(html_source)
  texts = soup.findAll(text=True)
  text = "\r\n".join(filter(visible, texts))
  return text


## EMail ################


def send_email_html(smtp,
                    email_user,
                    email_pwd,
                    to_address,
                    subject,
                    body_text,
                    images_jpg_path=[]):
  from email.mime.multipart import MIMEMultipart
  from email.mime.text import MIMEText
  from email.mime.image import MIMEImage

  # Define these once; use them twice!
  strFrom = email_user
  strTo = to_address  #must be a list

  # Create the root message and fill in the from, to, and subject headers
  msgRoot = MIMEMultipart('related')
  msgRoot['Subject'] = subject
  msgRoot['From'] = strFrom
  msgRoot['To'] = strTo
  msgRoot.preamble = 'This is a multi-part message in MIME format.'

  # Encapsulate the plain and HTML versions of the message body in an
  # 'alternative' part, so message agents can decide which they want to display.
  msgAlternative = MIMEMultipart('alternative')
  msgRoot.attach(msgAlternative)

  msgText = MIMEText(extract_text_from_html(body_text))
  msgAlternative.attach(msgText)

  # We reference the image in the IMG SRC attribute by the ID we give it below
  msgText = MIMEText(body_text, 'html')
  msgAlternative.attach(msgText)

  #Upload Images
  i = 0
  for img_path in images_jpg_path:
    fp = open(img_path, 'rb')
    msgImage = MIMEImage(fp.read())
    fp.close()
    i += 1
    msgImage.add_header('Content-ID', '<image' + str(i) + '>')
    msgRoot.attach(msgImage)

  # Send the email (this example assumes SMTP authentication is required)
  smtp.login(email_user, email_pwd)
  smtp.sendmail(strFrom, strTo, msgRoot.as_string())
  smtp.quit()


def send_email_exchange(to_address,
                        subject,
                        body_text,
                        sender='ocds.no-reply@thehartford.com',
                        attachments=[],
                        image_paths=[],
                        html=False):
  import smtplib
  from os.path import basename
  from email.mime.application import MIMEApplication
  from email.mime.multipart import MIMEMultipart
  from email.mime.text import MIMEText
  from email.utils import COMMASPACE, formatdate

  msg = MIMEMultipart('related') if html else MIMEMultipart()
  sender = sender
  to_address = to_address if isinstance(to_address, list) else [to_address]

  msg['From'] = sender
  msg['To'] = ','.join(to_address)
  msg['Subject'] = subject

  if html:
    msg.preamble = 'This is a multi-part message in MIME format.'
    msgAlternative = MIMEMultipart('alternative')
    msg.attach(msgAlternative)

    msgText = MIMEText(extract_text_from_html(body_text))
    msgAlternative.attach(msgText)

    msgText = MIMEText(body_text, 'html')
    msgAlternative.attach(msgText)

  else:
    msg.attach(MIMEText(body_text, 'plain'))

  for i, img_path in enumerate(image_paths):
    with open(img_path, 'rb') as fp:
      msgImage = MIMEImage(fp.read())
    msgImage.add_header('Content-ID', '<image' + str(i + 1) + '>')
    msg.attach(msgImage)

  for f in attachments:
    with open(f, "rb") as file:
      part = MIMEApplication(file.read(), Name=basename(f))
    part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
    msg.attach(part)

  # Send the message via our SMTP server
  SMTP_SERVER = os.getenv("SMTP_SERVER")
  if not SMTP_SERVER:
    log(Exception('Env SMTP_SERVER is not defined!'))

  s = smtplib.SMTP(SMTP_SERVER)
  s.sendmail(sender, to_address, msg.as_string())
  s.quit()
  log('Sent Email "{}" succesfully!'.format(subject))


## Web Hooks & API ################


def tk(app_name, action, name, value):
  "Track app actions for analysis"
  params = dict(
    AppName=app_name,
    Action=action,
    Name=name,
    Value=value,
  )

  URL = os.getenv('APP_TRACKER_URL')

  try:
    requests.post(URL, params=params)
  except:
    pass


def get_cookie_session_id(req):
  import random, string
  session_id = (req.cookies.get(cookie_session_key) or ''.join(
    random.SystemRandom().choice(string.ascii_uppercase + string.digits +
                                 string.ascii_lowercase) for _ in range(48)))
  return session_id


def process_request(req):
  val_dict = req.values.to_dict()
  form_dict = req.form
  data_dict = json.loads(req.data.decode('utf-8')) if req.data else {}
  return (val_dict, form_dict, data_dict)


def create_sio_client(host, port, response_map={}):
  """
  A Socket IO client
  https://pypi.org/project/socketIO-client/

  Example: 
  on_connect = lambda *args: print('connect')
  aaa_response = lambda *args: print('on_aaa_response', args)

  response_map = {
    'on_connect': on_connect,
    'aaa_response': aaa_response,
  }
  """
  from socketIO_client import SocketIO, LoggingNamespace
  sio_client = SocketIO(host, port, LoggingNamespace)
  for key in response_map:
    sio_client.on(key, response_map[key])

  return sio_client


class WebApp:
  '''
  A boilerplate for a web application using evenlet with flask & socketio.

  from xutil import WebApp
  app = WebApp(name='App1', port=5899)

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

  @app.on('disconnect')
  def disconnect(sid):
      print('disconnect ', sid)

  app.run()
  '''

  def __init__(self, name, port):
    import os, sys, time, json
    import socketio, socket
    from flask import Flask

    class MyFlask(Flask):
      jinja_options = Flask.jinja_options.copy()
      jinja_options.update(
        dict(
          block_start_string='(%',
          block_end_string='%)',
          variable_start_string='((',
          variable_end_string='))',
          comment_start_string='(#',
          comment_end_string='#)',
        ))

    self.name = name
    self.port = int(port)
    self.cookie_session_key = name + '_SID'
    self.sio = socketio.Server()
    self.flask_app = MyFlask(__name__)
    self.base_url = 'http://{}:{}'.format(socket.gethostname(), self.port)

    # Wrapper functions
    self.route = self.flask_app.route
    self.on = self.sio.on
    self.emit = self.sio.emit

  def run(self, debug=True):
    import eventlet, socketio
    import eventlet.wsgi

    # remember to use DEBUG mode for templates auto reload
    # https://github.com/lepture/python-livereload/issues/144
    self.flask_app.debug = debug

    app = socketio.Middleware(self.sio, self.flask_app)

    log('Web Server PID is {}'.format(os.getpid()))
    log("URL -> " + self.base_url)

    eventlet.wsgi.server(eventlet.listen(('0.0.0.0', self.port)), app)
