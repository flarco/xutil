# ETL Library
import sys, os, requests, time, json, datetime, getpass, argparse

import copy, shutil

from multiprocessing import Queue, Process

from xutil.helpers import (
  load_profile,
  log, log,
  now,
  now_str,
  split,
  get_exception_message,
  get_databases,
  get_kw,
  get_db_profile,
  struct)
from xutil.diskio import (
  read_yaml, read_file, write_csv,
  write_csvs, write_jsonl, write_jsonls,
  write_pqs
)

from xutil.web import send_email_exchange
import xutil.parallelism as parallelism

from xutil.database.base import (
  get_conn,
  DBConn)


from xutil.database.spark import (
  Spark,
  SparkConn,
)

from pathlib import Path



conns = {}


def exec_sql(
        database,
        sql=None,
        file_path=None,
        template_values=None,
        username=None,
        password=None,
        limit=None,
        delim=',',
        output_name=None,
        email=None,
        output_dir=None,
        query_name=None,
        echo=True,
        **kwargs
  ):
  """
  Function to execute SQL code on specified database

  Args:
      database: The database name to query (i.e: ORACLE_EDW, SPARK).
      sql: SQL snippet to execute.
      file_path: The source SQL file. Example: /path/to/file.sql
      template_values: A key / value list to fill a SQL template, separated by "&" and "=". i.e.:
      owner=USER1&tables=TABLE1,TABLE2
      username: The username to use to access database. Password will be prompted if not provided.
      password: password to use to access database.
      limit: he max number of records to pull.
      delim: The delimiter to use for output flat files. Default is ",".
      output_name: The file name to use. i.e.: "filename.csv".
      email: The email to send the output to. i.e.: "username@domain.com".
      output_dir: The output directory for the flat files. Default is "/path/to/folder".

  Returns:
      (field, rows) as a tuple if output_dir or email is not specified.
      field: is a list of the column names
      rows: is a list of tuples

  """
  global conns

  if file_path:
    sql = read_file(file_path)

  if template_values:
    template_values = template_values.split('&')
    kv_data = {kv.split('=')[0]: kv.split('=')[1] for kv in template_values}
    sql = sql.format(**kv_data)

  if output_dir:
    os.system("mkdir -p " + output_dir)

  if 'workflow' in kwargs and echo:
    print('***** EXECUTING SQL *****\n' + sql)

  dbs = get_databases()

  if database.upper() in conns:
    conn = conns[database.upper()]
  elif os.getenv('PROFILE_YAML'):
    conn = get_conn(
      database.upper(),
      use_spark=True,
      echo=True,
    )
  elif not os.getenv('PROFILE_YAML'):
    log("Environment variable 'PROFILE_YAML' not found!")
    log("Exiting.")
    sys.exit(1)
  else:
    log("Database is not supported!")
    log("Exiting.")
    sys.exit(1)

  conns[database.upper()] = conn
  fields, rows = conn.execute(
    sql,
    dtype='tuple',
    limit=limit,
    query_name=query_name,
    echo=echo
  )

  if fields and rows:

    if not (output_dir or email):
      return (fields, rows)

    user = os.getenv('USER')
    file_name = output_name if output_name else '{}-{}-{}.csv'.format(user, 'sql', int(time.time()))
    file_path = '{}/{}'.format(output_dir, file_name)
    write_csv(file_path, fields, rows, delim=delim)

    if email:
      if os.path.getsize(file_path) > 2 * (1024 ** 2):
        rc = os.system('gzip -f ' + file_path)
        file_path = file_path + '.gz' if rc == 0 else file_path

      subj = file_name
      body_text = 'See CSV attached\n\nROWS: {rows}\n\nSQL:\n{sql}'.format(rows=len(rows), sql=sql)

      send_email_exchange(
        to_address=email,
        subject="{} | {}".format(database, file_name),
        body_text=body_text,
        attachments=[file_path],
      )


def db_to_db(src_db,
             src_table,
             tgt_db,
             tgt_table,
             src_sql=None,
             where_clause='',
             tgt_mode='append',
             log=log,
             **kwargs):
  "Database to Database"
  print('= '*80)
  log('Launching data transfer Database to Database.')
  log('Source: {} | {}'.format(src_db, src_table))
  log('Target: {} | {}'.format(tgt_db, tgt_table))

  dbs = get_databases()
  if src_db not in dbs:
    raise Exception("Cannot find '{}' in database profile!".format(src_db))
  if tgt_db not in dbs:
    raise Exception("Cannot find '{}' in database profile!".format(tgt_db))

  src_db_prof = get_db_profile(src_db)
  tgt_db_prof = get_db_profile(tgt_db)

  use_jdbc = get_kw('use_jdbc', False, kwargs)
  hdfs_folder = get_kw('hdfs_folder', None, kwargs)
  partition_col = get_kw('partition_col', None, kwargs)
  partitions = int(get_kw('partitions', 5, kwargs))
  ff_only = get_kw('ff_only', False, kwargs)
  order_by = get_kw('order_by', [], kwargs)
  delete_after = get_kw('delete_after', False, kwargs)

  partition_col = partition_col if partition_col else get_partition_col(src_db, src_table)

  if tgt_db_prof.type in ('hive', 'spark') and src_db_prof.type not in (
      'hive', 'spark') and not use_jdbc:

    conn = get_conn(src_db)
    out_folder = conn.variables['tmp_folder']

    if src_table and partition_col and not src_sql:
      if ff_only:
        file_path = '{}/{}'.format(out_folder, src_table.lower())
      else:
        file_path, tot_cnt = db_table_to_ff_stream(
          src_db=src_db,
          table=src_table,
          partition_col=partition_col,
          where_clause=where_clause,
          out_folder=out_folder,
          partitions=partitions,
          max_running=partitions,
          log=log,
        )
    else:
      file_path = out_folder + '/' + tgt_table.lower() + '.csv'
      if not ff_only:
        src_sql = src_sql if src_sql else 'select * from ' + src_table
        data_strm = conn.stream(src_sql, rec_name=tgt_table)
        tot_cnt = write_csvs(file_path, data_strm, gzip=True, log=log)
      file_path = file_path + '.gz'

    ff_to_db(
      file_path,
      src_deli=',',
      src_timestamp_fmt='yyyy-MM-dd HH:mm:ss',
      src_date_fmt='yyyy-MM-dd',
      src_date_cols=[],
      src_pre_proc=None,
      tgt_db=tgt_db_prof.name,
      tgt_table=tgt_table,
      tgt_mode=tgt_mode,
      banner=False,
      log=log,
      delete_after=delete_after,
      hive_enabled=True,
      hdfs_folder=hdfs_folder,
      tot_cnt=tot_cnt,
    )
  else:
    # Use JDBC
    sparko = Spark(hive_enabled=False, spark_home=os.environ['SPARK_HOME'])
    if src_db_prof.type in ('hive', 'spark'):
      log("Reading Hive Table : " + src_table)
      sparko.sql('REFRESH TABLE ' + src_table)
      df1 = sparko.sql('select * from ' + src_table)
    else:
      src_table = '({})'.format(src_sql) if src_sql else src_table

      if partition_col:
        partition_dict = dict(
          partitionColumn=partition_col,
          numPartitions=partitions,
        )
      else:
        partition_dict = {}

      df1 = sparko.jdbc_read(
        src_db, src_table, partition_dict, log=log)
      tot_cnt = sparko._last_df_cnt

    sparko.jdbc_write(
      df1,
      tgt_db_prof,
      tgt_table,
      tgt_mode=tgt_mode,
      order_by=order_by,
      partitions=partitions,
      truncate=get_kw('truncate', None, kwargs),
      count_recs=get_kw('count_recs', None, kwargs),
      grant_sql=get_kw('grant_sql', None, kwargs),
      post_sql=get_kw('post_sql', None, kwargs),
      tot_cnt=tot_cnt,
      log=log)

  return dict(completed=True)



def db_to_ff(src_db, src_table, src_sql, tgt_ff, tgt_deli,
                 tgt_timestamp_fmt=None, tgt_date_fmt=None, where_clause='', log=log, **kwargs):
  "Database to Flat-File"
  if get_kw('banner', True, kwargs):
    print('= ' * 80)
    log('Launching data transfer Database to Flat-File.')
    log('Source: {} | {}'.format(src_db, src_table))
    log('Target: {}'.format(tgt_ff))

  dbs = get_databases()
  if src_db not in dbs:
    raise Exception("Cannot find '{}' in database profile!".format(src_db))

  src_db_prof = get_db_profile(src_db)

  partition_col = get_kw('partition_col', None, kwargs)
  stream_partitions = int(get_kw('stream_partitions', 5, kwargs))
  partitions = int(get_kw('partitions', 1, kwargs)) # final number of partitions


  if src_db_prof.type not in ('spark', 'hive'):
    sparko = Spark(hive_enabled=False)
    src_table = '({})'.format(src_sql) if src_sql else src_table
    if partition_col:
      partition_dict = dict(
        partitionColumn=partition_col, numPartitions=stream_partitions)
    else:
      partition_dict = {}
    df1 = sparko.jdbc_read(
      src_db, src_table, partition_dict, log=log)
    tot_cnt = sparko._last_df_cnt
  else:
    sparko = Spark(hive_enabled=True)
    log("Reading Hive Table : " + (src_table if src_table else 'QUERY'))
    sql = src_sql or 'select * from ' + src_table
    df1 = sparko.sql(sql)

  sparko.write_csv2(
    df1,
    tgt_ff,
    timestampFormat=tgt_timestamp_fmt,
    dateFormat=tgt_date_fmt,
    delimeter=tgt_deli,
    gzip=kwargs['gzip'] if 'gzip' in kwargs else True,
    partitions=partitions,
    log=log,
  )

  if 'tgt_post_proc' in kwargs and kwargs['tgt_post_proc'] in globals():
    globals()[kwargs['tgt_post_proc']](tgt_ff, log=log)

  return dict(completed=True)




def ff_to_db(src_ff, src_deli, tgt_db, tgt_table,
                 src_timestamp_fmt=None, src_date_fmt=None,
                 tgt_mode='append', log=log, **kwargs):
  "Flat-File to Database"
  if get_kw('banner', True, kwargs):
    print('= ' * 80)
    log('Launching data transfer Flat-File to Database.')
    log('Source: {}'.format(src_ff))
    log('Target: {} | {}'.format(tgt_db, tgt_table))

  if tgt_db not in get_databases():
    raise Exception("Cannot find '{}' in database profile!".format(tgt_db))

  tgt_db_prof = get_db_profile(tgt_db)

  hive_enabled = get_kw('hive_enabled', None, kwargs)
  hdfs_folder = get_kw('hdfs_folder', None, kwargs)
  src_date_cols = get_kw('src_date_cols', None, kwargs)
  order_by = get_kw('order_by', [], kwargs)
  src_pre_proc = get_kw('src_pre_proc', None, kwargs)
  delete_after = get_kw('delete_after', False, kwargs)

  if src_pre_proc and src_pre_proc in globals():
    globals()[src_pre_proc](src_ff, log=log)

  if tgt_db_prof.type in ('spark', 'hive'):
    conn = get_conn(tgt_db_prof.name)
    sparko = conn.sparko
  else:
    sparko = Spark(hive_enabled=False, version=2.2)
  log("Loading file {}..".format(src_ff))

  df1 = sparko.read_csv2(
    src_ff,
    delimeter=src_deli,
    timestampFormat=src_timestamp_fmt,
    dateFormat=src_date_fmt,
    date_cols=src_date_cols,
    hdfs_folder=hdfs_folder,
    log=log,
  )

  # To Hive
  if tgt_db_prof.type in ('hive', 'spark'):
    sparko.hive_write(df1, tgt_table, order_by=order_by, mode=tgt_mode, log=log)
    if delete_after:
      os.system('rm -rf ' + src_ff)

  # To RDBMS, use JDBC
  else:
    sparko.jdbc_write(
      df1,
      tgt_db_prof,
      tgt_table,
      tgt_mode=tgt_mode,
      order_by=order_by,
      partitions=get_kw('partitions', None, 5),
      truncate=get_kw('truncate', None, kwargs),
      count_recs=get_kw('count_recs', None, kwargs),
      grant_sql=get_kw('grant_sql', None, kwargs),
      post_sql=get_kw('post_sql', None, kwargs),
      tot_cnt=get_kw('tot_cnt', None, tot_cnt),
      log=log)

  return dict(completed=True)


def ff_to_ff(src_ff, src_deli,
                 src_timestamp_fmt, src_date_fmt, src_date_cols,
                 tgt_ff, tgt_deli, tgt_timestamp_fmt, tgt_date_fmt, log=log, **kwargs):
  "Flat-File to Flat-File"

  if 'src_pre_proc' in kwargs and kwargs['src_pre_proc'] in globals():
    globals()[kwargs['src_pre_proc']](src_ff, log=log)

  sparko = Spark(hive_enabled=False)

  # read
  df1 = sparko.read_csv2(
    src_ff,
    delimeter=src_deli,
    timestampFormat=src_timestamp_fmt,
    dateFormat=src_date_fmt,
    date_cols=src_date_cols,
    log=log,
  )

  # Write
  sparko.write_csv2(
    df1,
    tgt_ff,
    timestampFormat=tgt_timestamp_fmt,
    dateFormat=tgt_date_fmt,
    delimeter=tgt_deli,
    log=log,
  )

  if 'tgt_post_proc' in kwargs and kwargs['tgt_post_proc'] in globals():
    globals()[kwargs['tgt_post_proc']](tgt_ff, log=log)

  return dict(completed=True)


def get_partition_col(src_db, table, n=20000, echo=False):
  """Automatically detect a suitable partition column with n sample records"""
  conn = get_conn(src_db)
  field_rows = conn.get_columns(table, native_type=False)
  fields = [r.column_name.lower() for r in field_rows]
  number_fields_i = [i for i,r in enumerate(field_rows) if r.type in ('integer', 'double', 'decimal')]
  date_fields_i = [i for i,r in enumerate(field_rows) if r.type in ('datetime')]
  fields_i = date_fields_i + number_fields_i
  fields_stat = {
    fields[fi]: dict(nulls=0, uniques=set([]), min=None, max=None, ii=ii)
    for ii, fi in enumerate(fields_i)
  }

  date_conv = lambda f: conn.template('function.date_to_int').format(field=f)
  num_conv = lambda f: conn.template('function.number_to_int').format(field=f)
  fields_expr = [date_conv(fields[i])
                for i in date_fields_i] + [num_conv(fields[i]) for i in number_fields_i]
  fields_sql = ', '.join('{} as {}'.format(fexpr, fields[fields_i[ii]]) for ii, fexpr in enumerate(fields_expr))
  try:
    sql = conn.template('core.sample').format(fields=fields_sql, table=table, n=n)
    data = conn.select(sql, dtype='tuple', echo=echo)
  except Exception as E:
    err_msg = get_exception_message(raw=True).lower()
    if 'cannot select' in err_msg:
      log('-'+err_msg)
      log('-Retrying without Sampling')
      sql = conn.template('core.limit').format(
        fields=fields_sql, table=table, n=n)
      data = conn.select(sql, dtype='tuple', echo=echo)
    else:
      raise E


  fields = [f.lower() for f in conn._fields]

  if len(data) < n-2:
    return None

  for row in data:
    for fi, val in enumerate(row):
      f = fields[fi]
      fields_stat[f]['uniques'].add(val)
      if val is None:
        fields_stat[f]['nulls'] += 1
      else:
        if fields_stat[f]['min'] is None or val < fields_stat[f]['min']:
          fields_stat[f]['min'] = val
        if fields_stat[f]['max'] is None or val > fields_stat[f]['max']:
          fields_stat[f]['max'] = val

  for f in fields:
    fields_stat[f]['gap'] = float(fields_stat[f]['max'] - fields_stat[f]['min']) if fields_stat[f]['max'] and fields_stat[f]['min'] else 0
    fields_stat[f]['uniques_cnt'] = len(fields_stat[f]['uniques'])
    fields_stat[f][
      'uniques_prct'] = 100.0 * fields_stat[f]['uniques_cnt'] / len(data)
    fields_stat[f]['nulls_prct'] = 100.0 * fields_stat[f]['nulls'] / len(data)
    fields_stat[f]['count'] = len(data) - fields_stat[f]['nulls']
    # highest unique, lowest nulls
    fields_stat[f]['score'] = (100 - fields_stat[f]['nulls_prct']
                               ) * fields_stat[f]['uniques_prct'] / 100.0
    fields_stat[f][
      'score'] = 0 if fields_stat[f]['gap'] > 1000000 else fields_stat[f][
        'score']
    if echo:
      print('"{}" -> U{} | G{} | S{}'.format(f, fields_stat[f]['uniques_cnt'],
                                             fields_stat[f]['gap'],
                                             fields_stat[f]['score']))

  best_field = sorted(fields_stat, reverse=True, key=lambda f: fields_stat[f]['score'])[0]
  best_field_expr = fields_expr[fields_stat[best_field]['ii']]

  log('+Using partition col: '+best_field_expr)
  return best_field_expr



def get_sql_table_split(src_db, table, partition_col, partitions=10,
  where_clause='', cov_utf8=True, echo=False,):

  ######## Split and group the dates

  conn = get_conn(src_db, echo=echo, reconnect=True)

  split_data = conn.select(
    conn.template('routine.number_trunc_uniques').format(
      partition_col_trunc=conn.template('function.truncate_f').format(
        field=partition_col),
      where='where ' + where_clause if where_clause else '',
      table=table),
    rec_name=table if ' ' in partition_col else partition_col,
    echo=False)

  groups = []
  curr_group = []
  curr_group_size = 0
  total_cnt = sum([row.cnt for row in split_data])

  part_size = int(total_cnt / partitions)

  log('total_cnt is {}'.format(total_cnt))
  log('part_size is {} X {}'.format(part_size, partitions))

  ######## number of groups should equal partitions
  for row in split_data:
    val = row.trunc_field.strftime('%Y-%m-%d') if isinstance(
      row.trunc_field, datetime.datetime) else row.trunc_field
    curr_group.append(row.trunc_field)
    curr_group_size += row.cnt
    if curr_group_size > part_size:
      if curr_group_size > part_size * 2:
        log('-WARNING: Skewed data partition. curr_group_size({}) >  part_size * 2'.format(curr_group_size))
      groups.append(curr_group)
      curr_group = []
      curr_group_size = 0
  if curr_group:
    groups.append(curr_group)

  ######## get fields
  fields_rows = conn.get_columns(table, echo=False)
  fields_conv = []
  fields = []

  for row in fields_rows:
    field = row.column_name.lower()
    fields.append(field)

    if conn.type in ('oracle') and ('DATE' in row.type or 'TIME' in row.type
                                    or 'LONG' in row.type):
      fields_conv.append(field)
    elif conn.type in ('oracle') and cov_utf8:
      # to mitigate ORA-29275: partial multibyte character
      fields_conv.append((conn.template('function.str_utf8') +
                          " as {field}").format(field=field))
    else:
      fields_conv.append(row.column_name.lower())

  sql_templ = conn.template('routine.number_trunc_min_max')
  sqls = []
  for i, group in enumerate(groups):
    sql = sql_templ.format(
      table=table,
      fields=', '.join(fields_conv),
      where=where_clause + ' and ' if where_clause else '',
      partition_col_trunc=conn.template('function.truncate_f').format(
        field=partition_col),
      min_val=min([val for val in group if val]),
      max_val=max([val for val in group if val]),
      or_null=' or {} is null'.format(partition_col) if None in group else '')
    sqls.append(sql)

  return sqls, fields, total_cnt


def db_table_to_ff_stream(src_db,
                     table,
                     partition_col,
                     out_folder,
                     out_type='parquet',
                     where_clause='',
                     partitions=10,
                     file_name=None,
                     log=log,
                     echo=True,
                     **kwargs):
  s_t = datetime.datetime.now()
  log('Reading table "{}"'.format(table))

  gzip = get_kw('gzip', True, kwargs)
  cov_utf8 = get_kw('cov_utf8', True, kwargs)
  max_running = get_kw('max_running', partitions, kwargs)


  def sql_to_csv(src_db, sql, file_path, fields, queue=None, gzip=True):
    def log2(text, color=None):
      queue.put(('log', text))

    log2 = log2 if queue else log

    conn = get_conn(src_db, echo=False, reconnect=True)

    try:
      counter = write_csvs(
        file_path,
        conn.stream(sql, dtype='tuple', echo=False),
        fields=fields,
        gzip=gzip,
        log=log2)

      queue.put(('wrote-counter', counter))

    except Exception as E:
      log(E)
      queue.put(('exception', E))

  def sql_to_pq(src_db, sql, file_path, queue=None):
    def log2(text, color=None):
      queue.put(('log', text))

    log2 = log2 if queue else log

    conn = get_conn(src_db, echo=False, reconnect=True)

    try:
      counter = write_pqs(
        file_path,
        conn.stream(sql, dtype='dataframe', yield_chuncks=True, chunk_size=200000, echo=False),
        append=True,
        log=log2)

      queue.put(('wrote-counter', counter))

    except Exception as E:
      log(E)
      queue.put(('exception', E))

  # Split SELECT sql statements
  sqls, fields, total_cnt = get_sql_table_split(
    src_db,
    table,
    partition_col,
    partitions=partitions,
    where_clause=where_clause,
    cov_utf8=cov_utf8,
    echo=echo)

  ######## delete if exists and create folder
  table_folder = '{}/{}'.format(out_folder, file_name
                                if file_name else table.lower())
  os.system('rm -rf {f}; mkdir {f}'.format(f=table_folder))
  
  if os.path.exists(table_folder):
    shutil.rmtree(table_folder, ignore_errors=True)
  os.mkdir(table_folder)


  ######## Start the processes
  procs = []
  remaining_procs = []
  file_parts = []

  proc_items = {}
  for i, sql in enumerate(sqls):
    file_path = '{}/{}.{}'.format(table_folder, table.lower(),
                                     format(i, "03d"))
    file_parts.append(file_path)
    queue = Queue()
    if out_type == 'parquet':
      _proc = Process(
      target=sql_to_pq, args=(src_db, sql, table_folder, queue))
    else:
      _proc = Process(
      target=sql_to_csv, args=(src_db, sql, file_path + '.csv', fields, queue, gzip))

    proc_items[file_path] = dict(
      sql=sql, proc=_proc, queue=queue, prog_cnt=0)

    if len(procs) == max_running:
      remaining_procs.append(_proc)
    else:
      procs.append(_proc)
      procs[-1].start()

  ######## Keep track of streams
  done = []
  get_running_procs = lambda: [proc for proc in procs if proc.is_alive()]
  last_dt = datetime.datetime.now() - datetime.timedelta(seconds=30)
  last_prog_cnt = 0
  wrote_cnt = 0
  rate = 10000
  exptn = None

  while len(done) < len(file_parts):
    time.sleep(0.1)
    secs = round((datetime.datetime.now() - s_t).total_seconds(), 1)
    running_procs = get_running_procs()

    for file_path in proc_items:
      if not proc_items[file_path]['queue'].empty():
        # key can be log, counter, exception
        key, val = proc_items[file_path]['queue'].get()
        if key == 'wrote-counter':
          wrote_cnt += val
          done.append(file_path)
        if key == 'exception':
          log('Process exception for "{}":\n{}'.format(file_path, val))
          raise (val)
        if key == 'log':
          if 'rows' in val:
            proc_items[file_path]['prog_cnt'] = int(
              val.split(':')[1].split('rows')[0])

    secsd = (datetime.datetime.now() - last_dt).total_seconds()
    if secsd > 15:  # update each 15 sec
      prog_cnt = sum([item['prog_cnt'] for item in proc_items.values()])
      last_dt = datetime.datetime.now()
      rate = round((prog_cnt - last_prog_cnt) / secsd, 1)
      eta_mins = round((total_cnt - prog_cnt) / rate / 60,
                       1) if rate > 0 else '~'
      last_prog_cnt = prog_cnt

      text_log = '{elapsed_mins} m -- Streaming: {prog_cnt} / {total_cnt} [{rate} r/s] [{prct} %] --  {done_cnt} / {file_cnt} finished. {running_cnt} running. ETA is {eta_mins} mins'.format(
        elapsed_mins=round(secs / 60, 1),
        prog_cnt=prog_cnt,
        total_cnt=total_cnt,
        rate=rate,
        prct=round(100.0 * prog_cnt / total_cnt, 1),
        done_cnt=len(done),
        file_cnt=len(file_parts),
        running_cnt=len(running_procs),
        eta_mins=eta_mins)
      log(text_log)

    if len(running_procs) < max_running and remaining_procs:
      _proc = remaining_procs.pop(0)
      _proc.start()
      procs.append(_proc)

  secs = (datetime.datetime.now() - s_t).total_seconds()
  rate = round(wrote_cnt / secs, 1)
  if echo:
    log(" >>> Wrote {} rows in {} secs [{} r/s] to {}".format(
      wrote_cnt, secs, rate, table_folder))

  if wrote_cnt != total_cnt:
    log(
      '-WARNING: written count ({}) is different from initial total cnt ({}).'.
      format(wrote_cnt, total_cnt))
    log('-WARNING: total_cnt - wrote_cnt = {}'.format(total_cnt - wrote_cnt))

  return table_folder




etl_usage = '''
exec_etl.py <command> [<args>]
 
ETL commands available:
db-db     Database to Database transfer
ff-db     Flat-File to Database transfer
db-ff     Database to Flat-File transfer
 
Examples:
python exec_etl.py db-db --src_db=DB1 --src_table=schema_name1.table_name --tgt_db=HIVE --tgt_table=schema_name2.table_name
python exec_etl.py db-ff --src_db=DB1 --src_table=schema_name1.table_name --tgt_ff=/path/of/file.csv
python exec_etl.py ff-db --src_ff=/path/of/file.csv --tgt_db=HIVE --tgt_table=schema_name2.table_name
'''



class SqlCmdParser(object):
  def __init__(self):

    parser = argparse.ArgumentParser(description='Execute SQL from file or command line argument. Optional Output to Flat file.')
    parser.add_argument('--sql', help='SQL snippet to execute. Use double quote(").')
    parser.add_argument('--file_path', help='The source SQL file. Example: /path/to/file.sql')
    parser.add_argument('--template_values', help='A key / value list to fill a SQL template, separated by "&" and "=". i.e.: owner=SCHEMA_NM&tables=INVOICES,VENDORS')
    parser.add_argument('--database', help='The database name to query (i.e: EDW_1, SPARK).')
    parser.add_argument('--user', help='The username to use to access database. Password will be prompted.')
    parser.add_argument('--limit', help='The max number of records to pull.')
    parser.add_argument('--delim', help='The delimiter to use for output flat files. Default is ",".')
    parser.add_argument(
      '--output_dir',
      help='The output directory for the flat files. Default is "/tmp".')
    parser.add_argument('--output_name', help='The file name to use. i.e.: "Invoices_flat_wide.csv".')
    parser.add_argument('--email', help='The email to send the output to. i.e.: "tom@mydomain.com".')

    args = parser.parse_args()

    if not (args.database and (args.sql or args.file_path)):
      log("Need to specify database and (sql or filepath).", color='red')
      log("""Example: python /data/public/ds/scripts/exec-sql.py --sql='show databases;' --database=SPARK --output_name='Hive_databases.txt'""", color='green')
      parser.print_help()
      sys.exit(1)

    if args.database.upper() != 'SPARK' and not args.user and not os.getenv('PROFILE_YAML'):
      log("Need to specify user name with flag --user", color='red')
      log("Exiting.", color='red')
      sys.exit(1)

    exec_sql(
      database = args.database,
      file_path = args.file_path,
      sql = args.sql,
      template_values = args.template_values,
      username = args.user,
      limit = int(args.limit) if args.limit else None,
      delim = args.delim if args.delim else ',',
      output_name = args.output_name,
      email = args.email,
      output_dir = args.output_dir if args.output_dir else "/tmp",
    )


class EtlCmdParser(object):
  def __init__(self):
    parser = argparse.ArgumentParser(
      description='Execute ETL operations though CLI.', usage=etl_usage)
    parser.add_argument('command', help='ETL Subcommand to run')

    # parse_args defaults to [1:] for args, but you need to
    # exclude the rest of the args too, or validation will fail
    args = parser.parse_args(sys.argv[1:2])
    command = args.command.replace('-', '_')

    if not hasattr(self, command):
      print('Unrecognized command!')
      parser.print_help()
      exit(1)

    if not os.getenv('PROFILE_YAML'):
      log("Env Variable PROFILE_YAML must be set.")
      log(
        "PROFILE_YAML should be the path of the file containing database profiles / credentials."
      )
      log("Exiting.")
      sys.exit(1)

    # use dispatch pattern to invoke method with same name
    getattr(self, command)()

  def db_db(self):
    "Database to Database"
    parser = argparse.ArgumentParser(
      description='Database to Database transfer')

    # prefixing the argument with -- means it's optional
    parser.add_argument('--src_db', help='The source database name.')
    parser.add_argument(
      '--src_table', help='The source table name (including the schema).')
    parser.add_argument(
      '--src_sql',
      help=
      'The SQL snippet to execute from the source database. Use double quote(").'
    )
    parser.add_argument(
      '--src_sql_file',
      help=
      'The source SQL file to execute from the source database. Example: /path/to/file.sql'
    )
    parser.add_argument('--tgt_db', help='The target database name.')
    parser.add_argument(
      '--tgt_table', help='The target table name (including the schema).')
    parser.add_argument(
      '--append',
      help=
      'Append to the target table. Default will drop and overwrite the target table.'
    )

    # now that we're inside a subcommand, ignore the first
    args = parser.parse_args(sys.argv[2:])

    if not (args.src_db and
            (args.src_sql or args.src_sql_file or args.src_table)
            and args.tgt_db and args.tgt_table):
      log(
        "Need to specify src_db and (sql or filepath or src_table) and tgt_db and tgt_table."
      )
      parser.print_help()
      sys.exit(1)

    if args.src_sql_file:
      src_sql = read_file(args.src_sql_file)

    db_to_db(
      src_db=args.src_db,
      src_table=args.src_table,
      src_sql=src_sql,
      tgt_db=args.tgt_db,
      tgt_table=args.tgt_table,
      tgt_mode='append' if args.tgt_mode else 'overwrite',
    )

  def ff_db(self):
    "Flat File to Database"
    parser = argparse.ArgumentParser(
      description='Flat File to Database transfer')

    # prefixing the argument with -- means it's optional
    parser.add_argument('--src_ff', help='The source file path.')
    parser.add_argument(
      '--delim',
      help='The delimiter to use to parse the file. Default is ",".')
    parser.add_argument(
      '--datetime_format',
      help=
      'Datetime Format throughout the file. Default is "yyyy-MM-dd HH:mm:ss".')
    parser.add_argument(
      '--datetime_fields',
      help=
      'Datetime fields that should be parsed with specified datetime-format. ie: "date_field_1,date_field_2"'
    )
    parser.add_argument('--tgt_db', help='The target database name.')
    parser.add_argument(
      '--tgt_table', help='The target table name (including the schema).')
    parser.add_argument(
      '--tgt_mode', help='The target table insert mode (overwrite or append).')
    parser.add_argument(
      '--hdfs_folder',
      help=
      'The temporary HDFS field that the field should be stored in. Default is "/tmp"'
    )
    parser.add_argument(
      '--append',
      help='Append to existing table. Default is "overwrite".',
      action='store_true')

    # now that we're inside a subcommand, ignore the first
    args = parser.parse_args(sys.argv[2:])

    if not (args.src_ff and args.tgt_db and args.tgt_table):
      log("Need to specify src_ff and tgt_db and tgt_table.")
      parser.print_help()
      sys.exit(1)

    ff_to_db(
      src_ff=args.src_ff,
      src_deli=args.delim if args.delim else ',',
      src_timestamp_fmt=args.datetime_format if args.datetime_format else None,
      src_date_cols=args.datetime_fields.split(',')
      if args.datetime_fields else [],
      tgt_db=args.tgt_db,
      tgt_table=args.tgt_table,
      tgt_mode='append' if args.tgt_mode else 'overwrite',
      hdfs_folder=args.hdfs_folder if args.hdfs_folder else '/tmp',
    )

  def db_ff(self):
    "Database to Flat File"
    parser = argparse.ArgumentParser(description='Database to Flat File')

    # prefixing the argument with -- means it's optional
    parser.add_argument('--src_db', help='The source database name.')
    parser.add_argument(
      '--src_table', help='The source table name (including the schema).')
    parser.add_argument(
      '--src_sql',
      help=
      'The SQL snippet to execute from the source database. Use double quote(").'
    )
    parser.add_argument(
      '--src_sql_file',
      help=
      'The source SQL file to execute from the source database. Example: /path/to/file.sql'
    )
    parser.add_argument('--tgt_ff', help='The target file path.')
    parser.add_argument(
      '--delim',
      help='The delimiter to use to parse the file. Default is ",".')
    parser.add_argument(
      '--datetime_format',
      help=
      'Datetime Format throughout the file. Default is "yyyy-MM-dd HH:mm:ss".')

    # now that we're inside a subcommand, ignore the first
    args = parser.parse_args(sys.argv[2:])

    if not (args.src_db and
            (args.src_sql or args.src_sql_file or args.src_table)
            and args.tgt_ff):
      log(
        "Need to specify src_db and (sql or filepath or src_table) and tgt_ff."
      )
      parser.print_help()
      sys.exit(1)

    if args.src_sql_file:
      src_sql = read_file(args.src_sql_file)

    db_to_ff(
      src_db=args.src_db,
      src_table=args.src_table if args.src_table else None,
      src_sql=args.src_sql if args.src_sql else None,
      tgt_ff=args.tgt_ff,
      tgt_deli=args.delim if args.delim else ',',
      tgt_date_fmt=args.datetime_format if args.datetime_format else None,
      tgt_timestamp_fmt=args.datetime_format if args.datetime_format else None,
    )


def clean_file_1(file_path, log=log):
  log('Cleaning file {}'.format(file_path))
  os.system('sed -i "1s/ /_/g" {}'.format(file_path))  # fix first row
  os.system('sed -i "s/\\tNone/\\t/g" {}'.format(file_path))  # fix whole file


def remove_cr(file_path, log=log):
  log('Removing \\r character from {}'.format(file_path))
  os.system('sed -i "s/\\r//g" {}'.format(file_path))  # fix whole file


def gzip_file(file_path, log=log):
  log('Gzipping {}'.format(file_path))
  os.system('gzip -f {}'.format(file_path))


class Project:
  """
  Reprensents an ETL project, intaking a YAML file.
  """

  def __init__(self, name, path, **kwargs):
    self.name = name
    self.yaml_path = path
    self.data = read_yaml(path)
    self.variables = self.data['variables']
    self.echo = kwargs['echo'] if 'echo' in kwargs else False

    self.project_name = self.data['project'].replace(' ', '_')
    self.parse_tasks(echo=self.echo)

  def parse_tasks(self, echo=False):
    tasks = {}

    def prep_item(item, echo=False):
      # apply template as applicable
      if 'template' in item:
        for k in self.variables[item['template']]:
          item[k] = item.get(k, self.variables[item['template']][k])

      worker_name = 'worker-' + item['from'].lower() if 'from' in item else None

      # add missing keys
      item['naming_prefix'] = item.get('naming_prefix', '')
      item['naming_include_schema'] = item.get('naming_include_schema', True)
      item['tgt_schema'] = item.get('tgt_schema', '')
      item['date_column_map'] = item.get('date_column_map', '')
      item['order_column_map'] = item.get('order_column_map', '')
      item['where_clause'] = item.get('where_clause', '')
      item['where_clause_template'] = item.get('where_clause_template', '')
      item['grant_sql_template'] = item.get('grant_sql_template', '')
      item['post_sql_template'] = item.get('post_sql_template', '')
      item['gzip'] = item.get('gzip', False)
      item['folder'] = item.get('folder', None)
      item['grant_sql'] = item.get('grant_sql', '')
      item['post_sql'] = item.get('post_sql', '')
      item['partitions'] = item.get('partitions', 1)
      item['worker_filter'] = item.get('worker_filter', worker_name)
      item['order_field'] = []
      item['partition_col'] = None
      item['mode'] = item.get('mode', 'overwrite')
      item['delete_after'] = item.get('delete_after', False)

      item['pre_processor'] = item.get('pre_processor', '')
      item['post_processor'] = item.get('post_processor', '')

      return item

    # db-to-db ###############
    def process_db_to_db(echo=False):
      if echo: log('process_db_to_db')
      typ = 'db_to_db'

      for item in self.data[typ]:

        if echo: log('processing item: ' + str(item))
        item_ = prep_item(item)

        if 'table_list' in item:
          for n, table in enumerate(item_['table_list']):
            where_clause = None
            table_dict = table if isinstance(table, dict) else None

            if table_dict:
              where_clause = table_dict['where_clause'] if 'where_clause' in table_dict else where_clause
              table = table_dict['table']
            
            item = copy.deepcopy(item_)
            shema_nm = table.split('.')[0].replace('[', '').replace(']', '')
            table_nm = table.split('.')[-1]
            name = item['workflow'] + '.' + table_nm.lower() if 'workflow' in item else typ + '.' + table.lower()

            if echo: log('processing task: ' + name)

            tgt_table = '{}_{}'.format(shema_nm, table_nm).lower() if item[
              'naming_include_schema'] else table_nm.lower()
            tgt_table = item['naming_prefix'] + tgt_table
            tgt_table = table_dict['tgt_table'] if table_dict and 'tgt_table' in table_dict else tgt_table
            item['tgt_table'] = item['tgt_table'] if 'tgt_table' in item else tgt_table
            item['tgt_table'] = '{}.{}'.format(item['tgt_schema'], item['tgt_table']) if item['tgt_schema'] else item[
              'tgt_table']

            if echo: log('tgt_table: ' + item['tgt_table'])
            item['query'] = item['query'] if 'query' in item else None

            if item['date_column_map'] and 'column_map_date' in self.variables:
              item['partition_col'] = self.variables['column_map_date'][table] if table in self.variables[
                'column_map_date'] else None

            if item['order_column_map'] and 'column_map_order' in self.variables:
              item['order_field'] = self.variables['column_map_order'][table] if table in self.variables[
                'column_map_order'] else None

            if item['date_field'] and item['where_clause_template'] and item['where_clause_template'] in self.variables:
              where_clause = self.variables[item['where_clause_template']].format(
                col=item['partition_col'],
                start_date=self.variables['start_date'],
                end_date=self.variables['end_date'],
              )

            if item['where_clause'] and not where_clause:
              where_clause = item['where_clause']

            item['grant_sql'] = item['grant_sql_template'].format(table=item['tgt_table']) if item[
              'grant_sql_template'] else item['grant_sql']
            item['post_sql'] = item['post_sql_template'].format(table=item['tgt_table']) if item[
              'post_sql_template'] else item['post_sql']

            if name in tasks:
              raise Exception('workflow "{}" already in tasks data!'.format(name))
            else:
              tasks[name] = dict(
                n=n + 1,
                workflow=item['workflow'],
                name=name,
                type=typ,
                src_table=table,
                src_db=item['from'],
                tgt_db=item['to'],
                src_sql=item['query'],
                tgt_table=item['tgt_table'],
                tgt_mode=item['mode'],
                partition_col=item['partition_col'],
                order_by=item['order_field'],
                where_clause=where_clause,
                grant_sql=item['grant_sql'],
                post_sql=item['post_sql'],
                partitions=item['partitions'],
                worker_filter=item['worker_filter'],
                delete_after=item['delete_after'],
                _item=item,
              )

        elif 'query' in item and 'tgt_table' in item:
          item = copy.deepcopy(item_)
          n = 0
          table = item['tgt_table']
          name = item['workflow'] + '.' + table.lower() if 'workflow' in item else typ + '.' + table.lower()
          if echo: log('processing task: ' + name)

          item['tgt_table'] = '{}.{}'.format(item['tgt_schema'], item['tgt_table']) if item['tgt_schema'] else item[
            'tgt_table']

          item['query'] = item['query'] if 'query' in item else None
          if item['date_column_map'] and 'column_map_date' in self.variables:
            item['partition_col'] = self.variables['column_map_date'][table] if table in self.variables[
              'column_map_date'] else None

          if name in tasks:
            raise Exception('workflow "{}" already in tasks data!'.format(name))
          else:
            tasks[name] = dict(
              n=n + 1,
              workflow=item['workflow'],
              name=name,
              type=typ,
              src_table=table,
              src_db=item['from'],
              tgt_db=item['to'],
              src_sql=item['query'],
              tgt_table=item['tgt_table'],
              partition_col=item['partition_col'],
              order_by=item['order_field'],
              where_clause=item['where_clause'],
              grant_sql=item['grant_sql'],
              partitions=item['partitions'],
              worker_filter=item['worker_filter'],
              tgt_mode=item['mode'],
              delete_after=item['delete_after'],
              _item=item,
            )

        else:
          raise Exception('Item did not meet any condition: ' + str(item))

    def process_ff_to_db(echo=False):
      typ = 'ff_to_db'
      for n, item in enumerate(self.data[typ]):
        name = item['workflow'] + '.' + item['tgt_table'].lower() if 'workflow' in item else typ + '.' + item[
          'tgt_table'].lower()
        if echo: log('processing task: ' + name)

        item = prep_item(item)
        tgt_table = '{}.{}'.format(item['tgt_schema'], item['tgt_table'])

        tasks[name] = dict(
          n=n + 1,
          workflow=item['workflow'],
          name=name,
          type=typ,
          src_ff=item['file_path'],
          src_deli=item['delimiter'],
          src_timestamp_fmt=item['timestamp_format'],
          src_date_fmt=item['date_format'],
          src_date_cols=item['date_cols'],
          src_pre_proc=item['pre_processor'],
          order_by=item['order_by'],
          tgt_db=item['to'],
          tgt_table=tgt_table,
          tgt_mode=item['mode'],
          _item=item,
        )

    def process_db_to_ff(echo=False):
      typ = 'db_to_ff'
      for item in self.data[typ]:
        if echo: log('processing item: ' + str(item))
        item_ = prep_item(item)

        if 'table_list' in item:
          for n, table in enumerate(item['table_list']):
            item = copy.deepcopy(item_)
            shema_nm, table_nm = table.split('.')
            name = item['workflow'] + '.' + table_nm.lower() if 'workflow' in item else typ + '.' + table.lower()
            if echo: log('processing task: ' + name)

            item['query'] = item['query'] if 'query' in item else None
            if item['date_column_map'] and 'column_map_date' in self.variables:
              item['partition_col'] = self.variables['column_map_date'][table] if table in self.variables[
                'column_map_date'] else None

            file_name = '{}_{}'.format(shema_nm, table_nm).lower() if item[
              'naming_include_schema'] else table_nm.lower()
            file_name = item['naming_prefix'] + file_name
            item['file_path'] = '{}/{}.csv'.format(item['folder'], file_name)

            tasks[name] = dict(
              n=n + 1,
              workflow=item['workflow'],
              name=name,
              type=typ,
              src_db=item['from'],
              src_table=table,
              src_sql=item['query'],
              tgt_ff=item['file_path'],
              tgt_deli=item['delimiter'],
              tgt_timestamp_fmt=item['timestamp_format'],
              tgt_date_fmt=item['date_format'],
              tgt_post_proc=item['post_processor'],
              partition_col=item['partition_col'],
              partitions=item['partitions'],
              gzip=item['gzip'],
              worker_filter=item['worker_filter'],
              _item=item,
            )
        elif 'query' in item:
          item = copy.deepcopy(item_)
          n = 0
          table = item['tgt_table']
          name = item['workflow'] + '.' + table.lower() if 'workflow' in item else typ + '.' + table.lower()
          if echo: log('processing task: ' + name)

          item['tgt_table'] = '{}.{}'.format(item['tgt_schema'], item['tgt_table']) if item['tgt_schema'] else item[
            'tgt_table']

          item['query'] = item['query'] if 'query' in item else None
          if item['date_column_map'] and 'column_map_date' in self.variables:
            item['partition_col'] = self.variables['column_map_date'][table] if table in self.variables[
              'column_map_date'] else None

          if name in tasks:
            raise Exception('workflow "{}" already in tasks data!'.format(name))
          else:
            tasks[name] = dict(
              n=n + 1,
              workflow=item['workflow'],
              name=name,
              type=typ,
              src_db=item['from'],
              src_table=table,
              src_sql=item['query'],
              tgt_ff=item['file_path'],
              tgt_deli=item['delimiter'],
              tgt_timestamp_fmt=item['timestamp_format'],
              tgt_date_fmt=item['date_format'],
              tgt_post_proc=item['post_processor'],
              partition_col=item['partition_col'],
              partitions=item['partitions'],
              worker_filter=item['worker_filter'],
              _item=item,
            )
        else:
          raise Exception('Item did not meet any condition: ' + str(item))

    def process_db_sql(echo=echo):
      typ = 'db_sql'

      for item in self.data[typ]:
        if echo: log('processing item: ' + str(item))
        item_ = prep_item(item)

        if 'sql_list' in item_:
          for n, sql in enumerate(item_['sql_list']):
            name = item['workflow'] + '.' + str(n + 1)
            if name in tasks:
              raise Exception('workflow "{}" already in tasks data!'.format(name))
            else:
              tasks[name] = dict(
                n=n + 1,
                workflow=item['workflow'],
                name=name,
                type=typ,
                database=item['database'],
                sql=sql,
                worker_filter=item['worker_filter'],
                _item=item,
              )
        elif 'sql_file' in item_:
          item_['sql_file'] = item_['sql_file'] if isinstance(item_['sql_file'], list) else [item_['sql_file']]
          for n, file_path in enumerate(item_['sql_file']):
            if not Path(file_path).exists():
              raise Exception('Workflow {} SQL Path "{}" does not exists!'.format(
                item['workflow'], file_path
              ))

            name = '{}.{}.{}'.format(
              item['workflow'], str(n + 1), Path(file_path).name
            )
            tasks[name] = dict(
              n=n + 1,
              workflow=item['workflow'],
              name=name,
              type=typ,
              database=item['database'],
              file_path=file_path,
              worker_filter=item['worker_filter'],
              _item=item,
            )
        else:
          name = item['workflow']
          tasks[name] = dict(
            n=1,
            workflow=item['workflow'],
            name=name,
            type=typ,
            database=item['database'],
            sql=item['sql'],
            worker_filter=item['worker_filter'],
            _item=item,
          )

    def process_bash(echo=echo):
      typ = 'bash'

      for item in self.data[typ]:
        if echo: log('processing item: ' + str(item))
        name = item['workflow']
        tasks[name] = dict(
          n=1,
          name=name,
          workflow=item['workflow'],
          cmd=item['cmd'],
          type=typ,
          _item=item,
        )

    def process_jobs(echo=echo):
      typ = 'jobs'
      jobs = {}
      job_names = list(self.data[typ])
      for job_name in self.data[typ]:
        task_list = self.data[typ][job_name]
        for task_name in task_list:
          if not (task_name in self.task_names or task_name in self.workflow_names or task_name in job_names):
            raise Exception('Task name {} cannot be found!'.format(task_name))
          if job_name in tasks:
            raise Exception('Job name "{}" already in tasks data!'.format(job_name))

        jobs[job_name] = task_list

      return jobs

    if 'db_to_db' in self.data: process_db_to_db(echo=echo)
    if 'ff_to_db' in self.data: process_ff_to_db(echo=echo)
    if 'db_to_ff' in self.data: process_db_to_ff(echo=echo)
    if 'db_sql' in self.data: process_db_sql(echo=echo)
    if 'bash' in self.data: process_bash(echo=echo)

    self.tasks = tasks
    self.task_names = sorted([t for t in tasks])
    self.workflow_names = sorted(set([tasks[t]['workflow'] for t in tasks]))
    self.jobs = process_jobs(echo=echo) if 'jobs' in self.data else {}
    self.job_names = sorted(self.jobs)

  def get_tasks(self):
    sort_key = lambda x: (self.tasks[x]['type'], self.tasks[x]['workflow'], self.tasks[x]['n'])
    tasks = [self.tasks[wn] for wn in sorted(self.tasks, key=sort_key)]
    return tasks


etl_functions = dict(
  db_to_db=db_to_db,
  db_to_ff=db_to_ff,
  ff_to_db=ff_to_db,
  ff_to_ff=ff_to_ff,
  etl_db_sql=exec_sql,
  # bash=bash,
)
