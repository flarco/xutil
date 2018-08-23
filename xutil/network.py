# Network related lib
'''
Samba
SSH
HDFS
'''
import datetime
import socket
import sys
from .helpers import (log, slog, elog, get_exception_message, now)

import halo
Status = halo.Halo


class Server(object):
  def __init__(self, name, ip, username, password=None, key_path=None):
    self.port = 22
    if ":" in ip:
      self.port = int(ip.split(":")[-1])
      ip = ip.split(":")[0]

    import paramiko
    self.name = name
    self.ip = ip
    self.username = username
    self.password = password
    self.connect_tries = 0
    self.connected = False

    self.key = paramiko.RSAKey.from_private_key_file(
      key_path) if key_path else None

    self.ssh_client = paramiko.SSHClient()
    self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    self.status = Status(text='', spinner='dots')
    self.sftp = None

  def ssh_connect(self):
    self.connect_tries += 1

    try:
      with Status(text='Connecting to ' + self.ip, spinner='dots'):
        self.ssh_client.connect(
          self.ip,
          self.port,
          self.username,
          self.password,
          timeout=4,
          pkey=self.key)
        self.sftp = self.ssh_client.open_sftp()
      self.connected = True
      log("Successful Connection to " + self.name + " (%s)" % self.ip)

    except Exception as E:
      log("Failed to connect to {} ({})".format(self.name, self.ip))
      log(E)
    finally:
      return self.connected

  def ssh_command(self, command, wait_for_output=True):
    if not self.connected: self.ssh_connect()

    try:
      stdin, stdout, stderr = self.ssh_client.exec_command(command)
    except socket.error as e:
      # not connected?
      self.connected = False
      self.ssh_connect()
      stdin, stdout, stderr = self.ssh_client.exec_command(command)

    self.last_output = ''
    self.last_output_lines = 0

    if (wait_for_output):
      self.ssh_chan_status = stdout.channel.recv_exit_status()
      for line in stdout.readlines():
        self.last_output = self.last_output + line
        self.last_output_lines += 1

    return self.last_output

  def sftp_copy_from(self, remote_filepath, local_filepath):
    if (not self.test_connection): self.ssh_connect()
    # copy file from remote server object to local path
    log("Downloading from '" + remote_filepath + "' to '" + local_filepath +
        "'")
    try:
      self.status.start()
      self.last_stat = None
      self.sftp.get(
        remote_filepath, local_filepath, callback=self.transfer_progress)
      self.status.stop()
    except Exception as E:
      log('remote_filepath: {}:{}'.format(self.name, remote_filepath))
      log('local_filepath: {}'.format(local_filepath))
      log(E)

  def sftp_copy_to(self, local_filepath, remote_filepath):
    if (not self.test_connection): self.ssh_connect()
    # copy file from local path to remote server object
    log("Uploading from '" + local_filepath + "' to '" + remote_filepath + "'")
    try:
      self.status.start()
      self.last_stat = None
      self.sftp.put(
        local_filepath, remote_filepath, callback=self.transfer_progress)
      self.status.stop()
    except Exception as E:
      log('remote_filepath: {}:{}'.format(self.name, remote_filepath))
      log('local_filepath: {}'.format(local_filepath))
      log(E)

  def transfer_progress(self, transferred, total, unit='B'):
    "Display transfer progress"
    prct = int(100.0 * transferred / total)
    divide = lambda x, y: round(1.0 * x / (y), 1)

    if self.last_stat:
      secs = (datetime.datetime.now() - self.last_stat['time']).total_seconds()
      if secs > 2:
        rate = round((transferred - self.last_stat['transferred']) / secs, 1)
        self.last_stat = dict(time=now(), transferred=transferred, rate=rate)
      else:
        rate = self.last_stat['rate']
    else:
      rate = 0
      self.last_stat = dict(time=now(), transferred=transferred, rate=rate)

    if total > 1024**3:
      transferred = divide(transferred, 1024**3)
      total = divide(total, 1024**3)
      unit = 'GB'
      rate = '{} {} / sec'.format(divide(rate, 1024**2), 'MB')
    elif total > 1024**2:
      transferred = divide(transferred, 1024**2)
      total = divide(total, 1024**2)
      unit = 'MB'
      rate = '{} {} / sec'.format(divide(rate, 1024**2), unit)
    elif total > 1024**1:
      transferred = divide(transferred, 1024**1)
      total = divide(total, 1024**1)
      unit = 'KB'
      rate = '{} {} / sec'.format(divide(rate, 1024**1), unit)
    self.status.text = '{}% Complete: {} / {} {} @ {}'.format(
      prct, transferred, total, unit, rate)

  def test_connection(self):
    stdout = self.ssh_client.exec_command('ls')
    return self.connected
