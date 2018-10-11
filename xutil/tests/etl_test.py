# from parent folder, run 'python -m xutil.tests.etl_test'

from xutil import log


def test_db_table_to_ff_stream():
  from xutil import get_conn
  from xutil.database.etl import db_table_to_ff_stream
  csv_path = db_table_to_ff_stream(
    'ORCL_XENIAL',
    'HR.EMPLOYEES',
    "to_number(to_char(HIRE_DATE, 'j'))",
    out_folder='/tmp',
    gzip=False)


def test_stream(db, sql):
  from xutil import get_conn
  conn = get_conn(db, echo=False, reconnect=True)
  stream = conn.stream(sql, yield_batch=True, echo=False)
  for batch_rows in stream:
    print('Batch : {}'.format(len(batch_rows)))
  return conn_1._stream_counter


def stream_insert(args):
  (from_db, sql, to_db, tgt_table) = args
  from xutil import get_conn
  conn_1 = get_conn(from_db, echo=False, reconnect=True)
  conn_2 = get_conn(to_db, echo=False, reconnect=True)
  stream = conn_1.stream(sql, yield_batch=True, echo=False)

  for batch_rows in stream:
    conn_2.insert(tgt_table, batch_rows, echo=False)

  return conn_1._stream_counter


def test_db_todb_stream():
  from xutil import get_conn
  from xutil.database.etl import get_sql_table_split
  from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
  import datetime

  from_db = 'PG_XENIAL'
  to_db = 'ORCL_XENIAL'
  tgt_table = 'HR.orange_county_data'
  partitions = 2

  sqls = get_sql_table_split(
    src_db='PG_XENIAL',
    table='housing.orange_county_data',
    partition_col="trunc(extract(epoch from date_of_sale)/(60*60*24))::int",
    partitions=partitions)

  # sqls = sqls[:2]
  s_t = datetime.datetime.now()
  with ProcessPoolExecutor(len(sqls)) as pool:
    arg_tuples = [(from_db, sql, to_db, tgt_table) for sql in sqls]
    counters = pool.map(stream_insert, arg_tuples)
    tot_cnt = sum(counters)

  secs = (datetime.datetime.now() - s_t).total_seconds()
  rate = round(tot_cnt / secs, 1)
  log("Inserted {} records into '{}' [{} r/s]".format(tot_cnt, tgt_table,
                                                      rate))


def test_db_to_hive():
  from xutil.database.etl import db_table_to_ff_stream, db_to_db

  # Select from Oracle
  # Create into Hive via HDFS
  db_to_db(
    src_db='ORCL_XENIAL',
    src_table='HR.EMPLOYEES',
    tgt_db='SPARK_HIVE',
    tgt_table='default.HR_EMPLOYEES',
    partition_col="MANAGER_ID",
    out_folder='/tmp',
    hdfs_folder='tmp',
    partitions=5)

  # Select from PG
  # Create into Hive via HDFS
  # db_to_db(
  #   src_db='PG_XENIAL',
  #   src_table='housing.orange_county_data',
  #   tgt_db='SPARK_HIVE',
  #   tgt_table='default.orange_county_data',
  #   tgt_mode='overwrite',
  #   hdfs_folder='tmp',
  #   partition_col="trunc(extract(epoch from date_of_sale)/(60*60*24))::int",
  #   partitions=10)

  # Select from Oracle
  # Create in PG
  # db_to_db(
  #   src_db='ORCL_XENIAL',
  #   src_table='HR.EMPLOYEES',
  #   tgt_db='PG_XENIAL',
  #   tgt_table='public.HR_EMPLOYEES',
  #   partition_col="MANAGER_ID",
  #   tgt_mode='overwrite',
  #   partitions=5)

  return

  # Select from Oracle
  # Create in MSSQL
  # db_to_db(
  #   src_db='ORCL_XENIAL',
  #   src_table='HR.EMPLOYEES',
  #   tgt_db='MSSQL_XENIAL',
  #   tgt_table='dbo.HR_EMPLOYEES',
  #   partition_col="to_number(to_char(HIRE_DATE, 'j'))",
  #   tgt_mode='overwrite',
  #   partitions=5)

  # Select from PG
  # Create in Oracle
  # db_to_db(
  #   src_db='PG_XENIAL',
  #   src_table='public.HR_EMPLOYEES',
  #   tgt_db='ORCL_XENIAL',
  #   tgt_table='HR.EMPLOYEES2',
  #   tgt_mode='overwrite',
  #   partition_col="trunc(extract(epoch from HIRE_DATE)/(60*60*24))::int",
  #   partitions=5)

  db_to_db(
    src_db='PG_XENIAL',
    src_table='housing.orange_county_data',
    tgt_db='ORCL_XENIAL',
    tgt_table='HR.orange_county_data',
    tgt_mode='overwrite',
    partition_col="trunc(extract(epoch from date_of_sale)/(60*60*24))::int",
    partitions=10)

  # Select from MSSQL
  # Create in Oracle
  db_to_db(
    src_db='MSSQL_XENIAL',
    src_table='sys.all_objects',
    tgt_db='ORCL_XENIAL',
    tgt_table='HR.sys_all_objects',
    tgt_mode='overwrite',
    partition_col="CONVERT(INT, create_date)",
    partitions=5)

  # conn = get_conn('SPARK_HIVE')
  # data = conn.get_tables('default')
  # print(data)


def text_db_to_ff():

  ######## PG to Parquet
  from xutil import get_conn
  from xutil.diskio import write_pq, write_pqs
  from s3fs import S3FileSystem
  s3 = S3FileSystem()

  conn = get_conn('PG_XENIAL')
  df_chunks = conn.stream(
    'select * from housing.orange_county_data',
    dtype='dataframe',
  )
  write_pqs(
    '/tmp/housing.orange_county_data',
    df_chunks,
    # partition_cols=['property_zip'],
  )

  write_pqs(
    '/tmp/crypto.bittrex_prices',
    conn.stream(
      'select * from crypto.bittrex_prices',
      dtype='dataframe',
    ))

  write_pqs(
    's3://ocral-data-1/housing.landwatch',
    conn.stream(
      'select * from housing.landwatch',
      dtype='dataframe',
    ),
    filesystem=s3)

  write_pqs('/tmp/mining.places',
            conn.stream(
              'select * from mining.places',
              dtype='dataframe',
            ))


def test_hive_to_db():
  pass


if __name__ == '__main__':
  test_db_table_to_ff_stream()
  # test_db_to_hive()
  # test_db_todb_stream()
  # test_stream('PG_XENIAL', 'select * from housing.orange_county_data')
  # test_stream('ORCL_XENIAL', 'select * from HR.orange_county_data')