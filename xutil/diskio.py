import csv
import datetime
import os
import re
from collections import namedtuple
from zipimport import zipimporter
import psutil, shutil
import pyarrow as pa
import pyarrow.parquet as pq

import yaml

from yaml import (
  Loader,
  SafeLoader,
)

from xutil.helpers import log, now

try:
  from StringIO import StringIO, BytesIO
except:
  from io import StringIO, BytesIO  # python 3

try:
  from itertools import imap
except ImportError:  # Python 3
  imap = map


def construct_yaml_str(self, node):
  # Override the default string handling function
  # to always return unicode objects
  return self.construct_scalar(node)


Loader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)
SafeLoader.add_constructor(u'tag:yaml.org,2002:str', construct_yaml_str)


def write_yaml(path, dict_):
  """Save a dict to a YAML file"""
  text = yaml.dump(dict_, default_flow_style=False)
  write_file(path, text, echo=True)


def get_zip_path(path):
  for i, segment in enumerate(path.split('/')):
    if '.zip' in segment.lower():
      break
  zip_path = '/'.join(path.split('/')[:i + 1])
  return zip_path


def read_yaml(path):
  """Load from a YAML file, return dict"""
  if '.zip' in path.lower():
    Z = zipimporter(get_zip_path(path))
    file = BytesIO(Z.get_data(path))
    dict_ = yaml.load(file)
    return dict_

  with open(path) as file:
    dict_ = yaml.load(file)
  return dict_


def read_file(file_path, read_lines=False, mode='r', encoding='utf8'):
  """Read text from file"""
  if '.zip' in file_path.lower():
    Z = zipimporter(get_zip_path(file_path))
    stream = BytesIO(Z.get_data(file_path))
    return stream.read()

  encoding = None if 'b' in mode else encoding
  with open(file_path, mode, encoding=encoding) as stream:
    if read_lines:
      return stream.readlines()
    else:
      return stream.read()


def write_file(file_path,
               text,
               echo=False,
               append=False,
               mode=None,
               encoding='utf8'):
  """Write text to a file"""
  if echo: print('Saving to ' + file_path)
  mode = mode if mode else 'a' if append else 'w'
  f = open(file_path, mode, encoding=encoding)
  f.write(text)
  f.close()


def read_csv(file_path,
             delimiter=',',
             quotechar='"',
             mode='r',
             encoding="utf8"):
  """Read CSV from File"""
  s_t = now()

  with open(file_path, mode, encoding=encoding) as f:
    reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
    Data = namedtuple("Data", [f.replace(' ', '_') for f in next(reader)])
    # data_rows = [row for row in reader]
    try:
      i = 0
      data_rows = [row for i, row in enumerate(imap(Data._make, reader))]
    except Exception as e:
      print('ERROR at line ' + str(i + 1))
      raise e

  secs = (now() - s_t).total_seconds()
  rate = round(len(data_rows) / secs, 1)
  log("Imported {} rows from {} [{} r/s].".format(
    len(data_rows), file_path, rate))

  return data_rows


def read_csvD(file_path,
              delimiter=',',
              quotechar='"',
              date_cols=[],
              date_format=None,
              echo=True,
              recarray=False,
              detect_date=True):
  "Use Pandas DataFrame"
  # http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
  import pandas
  s_t = now()

  # https://stackoverflow.com/questions/17465045/can-pandas-automatically-recognize-dates
  def date_parser(x):
    dd = x
    try:
      dd = pandas.datetime.strptime(str(x), date_format)
    except ValueError:
      pass
    return dd

  date_parser = date_parser if date_format else None

  df = pandas.read_csv(
    file_path,
    delimiter=delimiter,
    parse_dates=date_cols,
    date_parser=date_parser,
    # quoting=csv.QUOTE_MINIMAL ,
    infer_datetime_format=detect_date,
    as_recarray=recarray)

  for col in df.columns:
    if not detect_date: continue
    if col in date_cols: continue
    if df[col].dtype == 'object':
      try:
        df[col] = pandas.to_datetime(df[col])
      except ValueError:
        pass

  replace_func = lambda col: re.sub(r'_+', '_', re.sub(r'[\]\[. ]', '_', col))
  df = df.rename(columns={col: replace_func(col) for col in df.columns})

  secs = (now() - s_t).total_seconds()
  rate = round(len(df) / secs, 1)
  if echo:
    log("Imported {} rows from {} [{} r/s].".format(len(df), file_path, rate))

  return df


def read_csvS(file_path,
              delimiter=',',
              headers=None,
              quotechar='"',
              encoding="utf8"):
  with open(file_path, 'r', encoding=encoding) as f:
    reader = csv.reader(f, delimiter=delimiter, quotechar=quotechar)
    Data = namedtuple("Row", next(reader)) if not headers else namedtuple(
      "Row", headers)
    cnt = 0
    for row in imap(Data._make, reader):
      cnt += 1
      yield row

  log("Imported {} rows from {}.".format(cnt, file_path))


def write_csv(file_path,
              headers,
              data,
              footer_text=None,
              append=False,
              log=log,
              echo=True,
              file_obj=None,
              encoding="utf8"):
  "Write to CSV, python3 compatible. 'data' must be list of iterables"
  s_t = now()
  mode = 'a' if append else 'w'
  f = file_obj if file_obj else open(
    file_path, mode, newline='', encoding=encoding)
  w = csv.writer(
    f, delimiter=',', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
  if not append: w.writerow(headers)
  for row in data:
    w.writerow(row)

  if footer_text:
    f.write(footer_text)

  if not file_obj: f.close()

  secs = (now() - s_t).total_seconds()
  rate = round(len(data) / secs, 1)
  if echo:
    log("Wrote {} rows to {} [{} r/s].".format(len(data), file_path, rate))


def write_csvs(file_path,
               data,
               fields=None,
               lineterminator='\n',
               deli=',',
               log=log,
               secs_d=10,
               gzip=False,
               to_str=False,
               encoding="utf8"):
  "Write to CSV, python3 compatible. 'data' must be list of interables"
  s_t = now()
  l_t = now()
  log_dlt = 100
  counter2 = 0
  f1 = open(file_path, 'w', newline='', encoding=encoding)
  w = csv.writer(
    f1,
    delimiter=deli,
    quoting=csv.QUOTE_MINIMAL,
    lineterminator=lineterminator)

  file_name = file_path.split('/')[-1]
  if fields:
    w.writerow(fields)
  else:
    row = next(data)
    w.writerow(row._fields)  # write headers
    w.writerow(row)  # write 1st row

  counter = 0
  for row in data:
    row = [str(v) for v in row] if to_str else row
    w.writerow(row)
    counter += 1
    counter2 += 1

    # progress message
    if counter2 % log_dlt == 0:
      secs_l = (now() - l_t).total_seconds()
      if secs_l >= secs_d:
        secs = (now() - s_t).total_seconds()
        rate = round(counter2 / secs_l, 1)
        mins = round(secs / 60, 1)
        log("{} min ## Writing to {}: {} rows @ {} r/s.".format(
          mins, file_name, counter, rate))
        l_t = now()
        counter2 = 0

  f1.close()

  if gzip:
    log('Gzipping ' + file_path)
    os.system('gzip -f ' + file_path)
    file_path = file_path + '.gz'

  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Wrote: {} rows to {} [{} r/s].".format(counter, file_path, rate))
  return counter


def write_pq(
    file_path,
    dataf,
    partition_cols=None,
    flavor='spark',
    filesystem=None,
    append=False,
    log=log,
):
  "Write to Parquet, python3 compatible. 'data' must be list of interables"
  s_t = now()

  if not append and os.path.exists(file_path):
    shutil.rmtree(file_path, ignore_errors=True)

  table = pa.Table.from_pandas(dataf, nthreads=psutil.cpu_count())
  counter = table.num_rows
  pq.write_to_dataset(
    table,
    root_path=file_path,
    partition_cols=partition_cols,
    flavor=flavor,
    preserve_index=False,
    filesystem=filesystem,
    use_deprecated_int96_timestamps=True,
    compression='snappy')  # will append. delete folder for overwrite

  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Wrote: {} rows to {} [{} r/s].".format(counter, file_path, rate))
  return counter


def write_pqs(
    file_path,
    dataf_chunks,
    partition_cols=None,
    flavor='spark',
    append=False,
    filesystem=None,
    log=log,
    secs_d=10,
):
  "Stream-Write to Parquet, python3 compatible. 'dataf_chunks' must be list of dataframes"
  s_t = now()
  l_t = now()
  log_dlt = 100
  counter = 0
  counter2 = 0

  if not append and os.path.exists(file_path):
    shutil.rmtree(file_path, ignore_errors=True)

  file_name = file_path.split('/')[-1]
  for dataf in dataf_chunks:
    table = pa.Table.from_pandas(dataf, nthreads=psutil.cpu_count())
    counter += table.num_rows
    counter2 += table.num_rows
    pq.write_to_dataset(
      table,
      root_path=file_path,
      partition_cols=partition_cols,
      flavor=flavor,
      preserve_index=False,
      filesystem=filesystem,
      use_deprecated_int96_timestamps=True,
      compression='snappy')  # will append. delete folder for overwrite

    secs_l = (now() - l_t).total_seconds()
    if secs_l >= secs_d:
      secs = (now() - s_t).total_seconds()
      rate = round(counter2 / secs_l, 1)
      mins = round(secs / 60, 1)
      log("{} min ## Writing to {}: {} rows @ {} r/s.".format(
        mins, file_name, counter, rate))
      l_t = now()
      counter2 = 0

  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Wrote: {} rows to {} [{} r/s].".format(counter, file_path, rate))
  return counter


def write_to_excel(file_path, sheet_order, dict_of_datas):
  'Write to Excel worksheets, dict_of_datas is "WHST_NAME":"DATA_NAMED_TUPLE"'
  from openpyxl import Workbook
  wb = Workbook()
  for sht_name in sheet_order:
    data = dict_of_datas[sht_name]
    sht_name = sht_name if len(sht_name) <= 31 else sht_name[-31:]
    ws = wb.create_sheet(title=sht_name)
    if not data: continue
    log(' >> Adding sheet "{}" with {} rows, {} columns'.format(
      sht_name, len(data), len(data[0])))
    ws.append(data[0]._fields)
    for i, row in enumerate(data):
      ws.append(row)

  wb.remove_sheet(wb.get_sheet_by_name('Sheet'))  # remove 1st default sheet
  wb.save(filename=file_path)
  log("Wrote {} sheets to {}.".format(len(sheet_order), file_path))


def get_hdfs(alias='lake'):
  # https://hdfscli.readthedocs.io/en/latest/api.html
  from hdfs import Config
  client = Config().get_client(alias)
  return client


def write_jsonl(file_path, data, log=log, encoding="utf8"):
  "Write JSONL to File"
  import jsonlines
  s_t = now()

  with open(file_path, 'w', encoding=encoding) as f:
    writer = jsonlines.Writer(f)
    writer.write_all(data)

  counter = len(data)
  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Wrote {} rows to {} [{} r/s].".format(counter, file_path, rate))


def write_jsonls(file_path, data, log=log):
  "Sream Write to JSON Lines. 'data' must be namedtuple. schema is a dict of field to data-type"
  import jsonlines
  s_t = now()
  l_t = now()
  msg_dlt = 10000
  counter = 0
  counter2 = 0

  with open(file_path, 'wb') as f:
    w = jsonlines.Writer(f)
    for row in data:
      w.write(row)
      counter += 1
      counter2 += 1

      # progress message
      if counter2 % msg_dlt == 0:
        secs_l = (now() - l_t).total_seconds()
        if secs_l >= 20:
          secs = (now() - s_t).total_seconds()
          rate = round(counter2 / secs_l, 1)
          mins = round(secs / 60, 1)
          log("{} min ## Writing to JSON: {} rows @ {} r/s.".format(
            mins, counter, rate))
          l_t = now()
          counter2 = 0

  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Wrote {} rows to {} [{} r/s].".format(counter, file_path, rate))


def read_jsonl(file_path, log=log, encoding="utf8"):
  "Read from JSONL File"
  import jsonlines
  s_t = now()

  with open(file_path, 'r', encoding=encoding) as f:
    reader = jsonlines.Reader(f)
    data = list(reader.iter())

  counter = len(data)
  secs = (now() - s_t).total_seconds()
  rate = round(counter / secs, 1)
  log("Read {} rows from {} [{} r/s].".format(counter, file_path, rate))

  return data


def get_path_size(p):
  from functools import partial
  prepend = partial(os.path.join, p)
  return sum([(os.path.getsize(f) if os.path.isfile(f) else get_path_size(f))
              for f in map(prepend, os.listdir(p))])
