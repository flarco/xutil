import datetime, os, sys, time, psutil, re, socket
from collections import OrderedDict, namedtuple
import socket
from pathlib import Path

from xutil.database.base import get_conn
from xutil.database.hive import HiveConn, Beeline
from xutil.database.oracle import OracleConn
from xutil.database.postgresql import PostgreSQLConn
from xutil.database.sqlite import SQLiteConn
from xutil.helpers import get_exception_message, now, log, struct, slog, get_profile, get_db_profile, get_kw, get_home_path
from xutil.diskio import read_yaml, write_jsonl, read_jsonl, get_path_size, read_file


class Spark:
  "Spark helper functions"

  version = None

  def __init__(self,
               app_name=None,
               master=None,
               conf={},
               spark_home=None,
               restart=False,
               hive_enabled=False,
               config_name=socket.gethostname().lower(),
               prog_handler=None,
               log=log):

    # restart = True if version != self.version else restart
    if os.getenv('KLOG'): os.system('bash $KLOG')  # kerb login
    spark_home = self.set_sparkenv(spark_home)

    from pyspark import SparkContext, SQLContext, SparkConf
    from pyspark.sql import SparkSession
    active_sc = SparkContext._active_spark_context

    if active_sc:
      log("Active SC ->> " + active_sc.appName)
      sc = active_sc
      spark = SparkSession(sc)
    else:
      sc = None
      spark = None

    if sc and restart:
      log('~Stopping Spark Instance ({})'.format(sc.appName))
      try:
        ps_data = {p.pid: p for p in psutil.process_iter() if p.cmdline()}
        child_pid = ps_data[os.getpid()].children()[0].pid
        if not hive_enabled:
          os.system('kill -9 ' + str(child_pid))
          SparkContext._gateway = None
      except:
        print(get_exception_message())

      sc.stop()
      sc = None

      # sc = sc.getOrCreate()

    profile = get_profile()
    if profile:
      conf_def = profile['spark-conf']
      if 'spark-conf-name' in profile:
        if config_name in profile['spark-conf-name']:
          # overwrite the default spark-conf
          for key in profile['spark-conf-name'][config_name]:
            conf_def[key] = profile['spark-conf-name'][config_name][key]
    else:
      conf_def = {
        "spark.master": "local[4]",
        "spark.driver.memory": "5g",
        "spark.driver.maxResultSize": "2g",
        "spark.driver.cores": "1",
        "spark.executor.instances": "4",
        "spark.executor.cores": "4",
        "spark.sql.broadcastTimeout": 900,
        # "spark.sql.tungsten.enabled": "true",
        "spark.io.compression.codec": "snappy",
        "spark.rdd.compress": "true",
        "spark.streaming.backpressure.enabled": "true",
        "spark.sql.parquet.compression.codec": "snappy",
      }

    # set extraClassPath
    conf_def["spark.driver.extraClassPath"] = self._get_jar_paths(profile)
    if 'SPARK_CLASSPATH' in os.environ and os.environ['SPARK_CLASSPATH']:
      conf_def["spark.driver.extraClassPath"] = conf_def["spark.driver.extraClassPath"] + ':' + os.environ['SPARK_CLASSPATH']
      del os.environ['SPARK_CLASSPATH']

    if master: conf['spark.master'] = master
    if hive_enabled: conf["spark.sql.catalogImplementation"] = "hive"

    for c in conf_def:
      conf[c] = conf_def[c] if c not in conf else conf[c]

    # Launch Spark Instance
    version = self.get_spark_version(spark_home)

    app_name = app_name if app_name else 'Spark_{}_{}_{}'.format(
      str(version).replace('.', ''), os.getenv('USER'), os.getpid())

    if not sc:
      log('Starting Spark Instance ({}) with version {} / {}'.format(
        app_name, version, conf['spark.master']))
      sc, spark, proc = self.init_spark(app_name, spark_home, hive_enabled, conf, restart, prog_handler)
      self.proc = proc

    self.hive_enabled = hive_enabled
    self.version = version
    self.sc = sc
    self.uiWebUrl = sc.uiWebUrl
    self.local_uiWebUrl = 'http://{}:{}'.format(socket.gethostname(), sc.uiWebUrl.split(':')[-1])
    self.spark = spark


  @classmethod
  def set_sparkenv(cls, spark_home=None):
    import urllib.request, subprocess
    URL = 'http://apache.claz.org/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz'
    spark_home = spark_home or os.environ.get('SPARK_HOME', None)

    if not spark_home:
      # download Spark binary
      ans = input(
        'SPARK_HOME is not found. Would you like download and install in your home folder? (Y or N): '
      )
      if ans.lower() == 'y':
        archive_path = get_home_path() + '/' + URL.split('/')[-1]
        if Path(archive_path).exists():
          log('+Archive "{}" already downloaded.'.format(archive_path))
        else:
          log('+Downloading "{}" to "{}"'.format(URL, archive_path))
          urllib.request.urlretrieve(URL, archive_path)
          os.system('cd {} && tar -xf {}'.format(get_home_path(),
                                                 archive_path))
          pyspark_home = archive_path.replace('.tgz', '/python')
          subprocess.call(
            ['pip', 'install', '--target=' + pyspark_home, 'py4j'])
        spark_home = archive_path.replace('.tgz', '')

    os.environ['SPARK_HOME'] = spark_home

    if not Path(spark_home).exists():
      raise Exception('SPARK_HOME="{}" does not exists.'.format(spark_home))

    # add pyspark of SPARK_HOME to path
    sys.path.append(spark_home + '/python')

    log('-Using SPARK_HOME=' + spark_home)

    return spark_home


  def get_master(self):
    return self.spark.sparkContext.master

  def get_spark_version(self, spark_home):
    try:
      line = read_file(spark_home + '/RELEASE', read_lines=True)[0]
      version = line.split()[1]
    except Exception as E:
      log(E)
      log('-Unable to determine Spark Version.')
      version = 'x.x'
    return version

  def get_app_name(self):
    return self.spark.sparkContext.appName

  def init_spark(self,
                 app_name,
                 sparkHome,
                 hive_enabled,
                 conf={},
                 restart=False,
                 prog_handler=None):
    from pyspark import SparkContext, SQLContext, SparkConf
    from pyspark.sql import SparkSession, HiveContext

    sparkconf = SparkConf()
    for key in conf:
      sparkconf.set(key, conf[key])

    if restart:
      SparkContext._gateway = None

    gateway = proc = None
    # gateway, proc = self._launch_gateway(conf=sparkconf)
    # self._launch_gateway_output_handler(proc, prog_handler)
    sc = SparkContext(
      appName=app_name, sparkHome=sparkHome, conf=sparkconf, gateway=gateway)

    if hive_enabled:
      hiveContext = HiveContext(sc)
      spark = SparkSession.builder.appName(app_name).config(
        conf=sparkconf).enableHiveSupport().getOrCreate()
    else:
      spark = SparkSession(sc)


    return sc, spark, proc

  def _get_jar_paths(self, profile):
    from xutil.database.jdbc import get_jar_path
    if 'drivers' not in profile:
      log(Exception('"drivers" key not in profile!'))
      return

    jar_paths = []
    for db_type in profile['drivers']:
      jar_path = get_jar_path(db_type, profile)
      jar_paths.append(jar_path)

    return ':'.join(jar_paths)

  def _set_spark_classpath(self, profile):
    jar_paths = self._get_jar_paths(profile)
    os.environ['SPARK_CLASSPATH'] = '{}:{}'.format(
      jar_paths, os.environ['SPARK_CLASSPATH']
      if 'SPARK_CLASSPATH' in os.environ else '')

  def _launch_gateway_output_handler(self, proc, prog_handler):
    from threading import Thread
    from multiprocessing import Process, Queue, Lock
    import select, fcntl, signal, atexit

    global exit_queue

    exit_queue = Queue()

    def stream_print(proc, exit_queue, prog_handler):

      def spark_log_handler(text, handle=lambda x:x):
        import re
        handle = lambda x: x if not handle else handle
        regex = r"\[Stage (\d+):(=*)>( *)\((\d+) \+ (\d+)\) \/ (\d+)"
        matches = list(re.finditer(regex, text, re.MULTILINE))
        if len(matches) == 0:
          print(text)
          return

        for matchNum, match in enumerate(matches):
          items = []
          for groupNum in range(1, len(match.groups())+1):
            items += [match.group(groupNum)]
          if len(items) == 6:
            items[1] = len(items[1])
            items[2] = len(items[2])
            prog_dict = dict(
              stage=items[0],
              complete=int(items[3]),
              running=int(items[4]),
              total=int(items[5]),
              percent=int(100*int(items[3])/int(items[5])))
            handle(prog_dict)
            log('Spark processing Stage #{}: {}% / {} Running'.format(prog_dict['stage'], prog_dict['percent'], prog_dict['running']))

      def non_block_read(output):
        fd = output.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        try:
          return output.read().decode(errors='replace').rstrip()
        except:
          return ""

      while exit_queue.empty():
        time.sleep(0.05)
        val = non_block_read(proc.stdout)
        if val: spark_log_handler(val, prog_handler)

    # does not exit properly if Thread is not ended
    # https://stackoverflow.com/a/3765160/2295355
    # self.proc_stdout = Thread(target=stream_print, args=(proc, exit_queue, prog_handler))

    self.proc_stdout = Process(
      target=stream_print, args=(proc, exit_queue, prog_handler))

    def signal_handler(*args, **kwargs):
      global exit_queue
      exit_queue.put(1)

    atexit.register(signal_handler)

    self.proc_stdout.start()

  def _launch_gateway(self, conf=None):
    """
    launch jvm gateway
    :param conf: spark configuration passed to spark-submit
    :return:
    """
    import atexit
    import os
    import sys
    import select
    import signal
    import shlex
    import socket
    import platform
    from subprocess import Popen, PIPE, STDOUT

    if sys.version >= '3':
      xrange = range

    from py4j.java_gateway import java_import, JavaGateway, GatewayClient
    from pyspark.find_spark_home import _find_spark_home
    from pyspark.serializers import read_int

    if "PYSPARK_GATEWAY_PORT" in os.environ:
      gateway_port = int(os.environ["PYSPARK_GATEWAY_PORT"])
    else:
      SPARK_HOME = _find_spark_home()
      # Launch the Py4j gateway using Spark's run command so that we pick up the
      # proper classpath and settings from spark-env.sh
      on_windows = platform.system() == "Windows"
      script = "./bin/spark-submit.cmd" if on_windows else "./bin/spark-submit"
      command = [os.path.join(SPARK_HOME, script)]
      if conf:
        for k, v in conf.getAll():
          command += ['--conf', '%s=%s' % (k, v)]
      submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "pyspark-shell")
      if os.environ.get("SPARK_TESTING"):
        submit_args = ' '.join(["--conf spark.ui.enabled=false", submit_args])
      command = command + shlex.split(submit_args)

      # Start a socket that will be used by PythonGatewayServer to communicate its port to us
      callback_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      callback_socket.bind(('127.0.0.1', 0))
      callback_socket.listen(1)
      callback_host, callback_port = callback_socket.getsockname()
      env = dict(os.environ)
      env['_PYSPARK_DRIVER_CALLBACK_HOST'] = callback_host
      env['_PYSPARK_DRIVER_CALLBACK_PORT'] = str(callback_port)

      # Launch the Java gateway.
      # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
      if not on_windows:
        # Don't send ctrl-c / SIGINT to the Java gateway:
        def preexec_func():
          signal.signal(signal.SIGINT, signal.SIG_IGN)

        proc = Popen(
          command,
          stdin=PIPE,
          stdout=PIPE,
          stderr=STDOUT,
          preexec_fn=preexec_func,
          env=env)
      else:
        # preexec_fn not supported on Windows
        proc = Popen(command, stdin=PIPE, env=env)

      gateway_port = None
      # We use select() here in order to avoid blocking indefinitely if the subprocess dies
      # before connecting
      while gateway_port is None and proc.poll() is None:
        timeout = 1  # (seconds)
        readable, _, _ = select.select([callback_socket], [], [], timeout)
        if callback_socket in readable:
          gateway_connection = callback_socket.accept()[0]
          # Determine which ephemeral port the server started on:
          gateway_port = read_int(gateway_connection.makefile(mode="rb"))
          gateway_connection.close()
          callback_socket.close()
      if gateway_port is None:
        raise Exception(
          "Java gateway process exited before sending the driver its port number"
        )

      # In Windows, ensure the Java child processes do not linger after Python has exited.
      # In UNIX-based systems, the child process can kill itself on broken pipe (i.e. when
      # the parent process' stdin sends an EOF). In Windows, however, this is not possible
      # because java.lang.Process reads directly from the parent process' stdin, contending
      # with any opportunity to read an EOF from the parent. Note that this is only best
      # effort and will not take effect if the python process is violently terminated.
      if on_windows:
        # In Windows, the child process here is "spark-submit.cmd", not the JVM itself
        # (because the UNIX "exec" command is not available). This means we cannot simply
        # call proc.kill(), which kills only the "spark-submit.cmd" process but not the
        # JVMs. Instead, we use "taskkill" with the tree-kill option "/t" to terminate all
        # child processes in the tree (http://technet.microsoft.com/en-us/library/bb491009.aspx)
        def killChild():
          Popen(["cmd", "/c", "taskkill", "/f", "/t", "/pid", str(proc.pid)])

        atexit.register(killChild)

    # Connect to the gateway
    gateway = JavaGateway(GatewayClient(port=gateway_port), auto_convert=True)

    # Import the classes used by PySpark
    java_import(gateway.jvm, "org.apache.spark.SparkConf")
    java_import(gateway.jvm, "org.apache.spark.api.java.*")
    java_import(gateway.jvm, "org.apache.spark.api.python.*")
    java_import(gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # TODO(davies): move into sql
    java_import(gateway.jvm, "org.apache.spark.sql.*")
    java_import(gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(gateway.jvm, "scala.Tuple2")

    return gateway, proc

  def get_type_map(self):
    from pyspark.sql.types import (StringType, TimestampType, DoubleType,
                                   BooleanType, IntegerType, LongType,
                                   StructType, StructField, ArrayType, MapType)

    type_map = {
      'int':
      IntegerType(),
      'integer':
      IntegerType(),
      'bigint':
      LongType(),
      'long':
      LongType(),
      'text':
      StringType(),
      'string':
      StringType(),
      'date':
      TimestampType(),
      'datetime':
      TimestampType(),
      'timestamp':
      TimestampType(),
      'float':
      DoubleType(),
      'double':
      DoubleType(),
      'decimal':
      DoubleType(),
      'boolean':
      BooleanType(),
      'array<string>':
      ArrayType(StringType()),
      'array<bigint>':
      ArrayType(LongType()),
      'array<int>':
      ArrayType(IntegerType()),
      'array<date>':
      ArrayType(TimestampType()),
      'array<timestamp>':
      ArrayType(TimestampType()),
      'array<float>':
      ArrayType(DoubleType()),
      'array<double>':
      ArrayType(DoubleType()),
      'array<decimal>':
      ArrayType(DoubleType()),
      'map<string,array<string>>':
      MapType(StringType(), ArrayType(StringType())),
    }

    return type_map

  def df_to_field_types(self, df, n=50000, data_map=None):
    "Get Field:Types from dataframe dtypes"
    from pyspark.sql.types import StringType

    data_map = data_map if data_map else {
      'int64': 'integer',
      'int': 'integer',
      'bigint': 'integer',
      'float64': 'decimal',
      'float': 'decimal',
      'double': 'decimal',
      'decimal': 'decimal',
      'date': 'date',
      'datetime64[ns]': 'datetime',
      'timestamp': 'timestamp',
      'object': 'string',
      'string': 'string',
      'boolean': 'string',
    }

    # df = df.head(n)
    clean_dftype = lambda t: t.split('(')[0] if '(' in t else t
    type_map = {f: data_map[clean_dftype(df.dtypes[i][1])] for i, f in enumerate(df.columns)}
    field_types = OrderedDict()

    df.registerTempTable("df")

    df_max = self.spark.sql('select {} from df'.format(',\n'.join([
      'max(length({f})) as {f}'.format(f=f) for f in df.columns
    ]))).head().asDict()

    for col in df.columns:
      col_ = col.replace(' ', '_')
      if type_map[col] != 'datetime':
        max_len = df_max[col]

      if type_map[col] in ('date', 'datetime', 'timestamp'):
        field_types[col_] = (type_map[col], 0, 0)
      elif type_map[col] in ('integer'):
        max_len = max_len if max_len else 10
        field_types[col_] = (type_map[col], max_len + 1, 0)
      elif type_map[col] in ('string'):
        max_len = max_len if max_len else 100
        if max_len > 2000:
          type_map[col] = 'text'
          max_len = 0
        elif max_len > 1000:
          max_len = 2000
        field_types[col_] = (type_map[col], max_len * 2, 0)
      elif type_map[col] in ('decimal'):
        max_len = max_len if max_len else 5
        field_types[col_] = (type_map[col], max_len + 8, 7)

    return field_types

  def create_schema(self, fields=None, types=None, schema_path=None, log=log):
    "Create Spark schema from lists 'fields' and 'types'"

    from pyspark.sql.types import (
      StructType,
      StructField,
    )

    type_map = self.get_type_map()

    if schema_path is not None:
      schema_data = read_jsonl(schema_path, log=log)
      fields = [r[0] for r in schema_data]
      types = [r[1] for r in schema_data]

    return StructType(
      [StructField(f, type_map[types[i]], True) for i, f in enumerate(fields)])
    # return [[f, type_map[types[i]]] for i, f in enumerate(fields)]

  def create_df(self, data, fields, types=None):
    """
    data = list of tuples or namedtuples
    fields = column names
    type = field types. If None, all will be String.

    df1 = sparko.create_df(
      [('a',2,3), ('b',5,6)],
      ['f1','f2','f3'],
      ['string', 'int', 'int']
    )
    """
    data1 = self.sc.parallelize(data)
    if types == None: types = ['string' for f in fields]

    schema = self.create_schema(fields, types)

    return self.spark.createDataFrame(data1, schema)

  def create_ngram(self, df, n, input_col, output_col='ngrams'):
    "Generate N-Gram -> https://spark.apache.org/docs/2.2.0/ml-features.html#n-gram"
    from pyspark.ml.feature import NGram

    ngram = NGram(n=n, inputCol=input_col, outputCol=output_col)

    ngram_df = ngram.transform(df)
    return ngram_df

  def remove_stop_words(self, df1, input_col, output_col='filtered'):
    "Remove stop words -> https://spark.apache.org/docs/2.2.0/ml-features.html#stopwordsremover"
    from pyspark.ml.feature import StopWordsRemover
    remover = StopWordsRemover(inputCol=input_col, outputCol=output_col)
    df2 = remover.transform(df1)

    return df2

  def tokenize(self, df1, input_col, output_col='words', pattern=None):
    "Tokenize string -> https://spark.apache.org/docs/2.2.0/ml-features.html#tokenizer"
    from pyspark.ml.feature import Tokenizer, RegexTokenizer

    if pattern:
      tokenizer = RegexTokenizer(
        inputCol=input_col, outputCol=output_col, pattern=pattern)
    else:
      tokenizer = Tokenizer(inputCol=input_col, outputCol=output_col)

    tokenized_df = tokenizer.transform(df1)

    return tokenized_df

  @staticmethod
  def hdfs_put(file_path,
               hdfs_folder,
               put=True,
               rm=True,
               log=log):
    "Move file from local to HDFS"
    file_size = get_path_size(file_path)
    file_name = file_path.split('/')[-1]
    hdfs_file_path = '{}/{}'.format(hdfs_folder, file_name)
    s_t = now()
    if rm:
      os.system('hdfs dfs -rm -r -f -skipTrash {}'.format(hdfs_file_path))
    if put:
      os.system('hdfs dfs -put -f {} {}'.format(file_path, hdfs_folder))
      secs = (now() - s_t).total_seconds()
      mins = round(secs / 60, 1)
      rate = round(file_size / 1024**2 / secs, 1)
      log("Moved data into HDFS in {} mins [{}MB -> {} MB/s]".format(mins, file_size, rate))
    return hdfs_file_path

  def read_json(self,
                hdfs_file_path,
                schema,
                timestampFormat='yyyy-MM-dd HH:mm:ss',
                dateFormat='yyyy-MM-dd',
                date_cols=[],
                timestamp_cols=[],
                log=log):
    "Read from json file on HDFS"

    df = self.spark.read.json(
      hdfs_file_path,
      # timestampFormat=timestampFormat,
      # dateFormat=dateFormat,
      schema=schema,
      # multiLine=multiLine,
      mode='FAILFAST',
    )

    df = self.process_df_fields(df, date_cols, timestamp_cols, dateFormat,
                                timestampFormat)

    return df

  def read_json2(self,
                 file_path,
                 hdfs_folder,
                 schema_path=None,
                 timestampFormat='yyyy-MM-dd HH:mm:ss',
                 dateFormat='yyyy-MM-dd',
                 date_cols=[],
                 timestamp_cols=[],
                 hdfs_put=True,
                 log=log):
    "Read from json file initially on local"

    schema_path = schema_path if schema_path else file_path + '.schema'
    schema_data = read_jsonl(schema_path, log=log)
    schema = self.create_schema([r[0] for r in schema_data],
                                [r[1] for r in schema_data])

    hdfs_file_path = self.hdfs_put(file_path, hdfs_folder, put=hdfs_put)

    return self.read_json(
      hdfs_file_path,
      schema=schema,
      timestampFormat=timestampFormat,
      dateFormat=dateFormat,
      date_cols=date_cols,
      timestamp_cols=timestamp_cols,
      log=log,
    )

  def read_csv2(self,
                file_path=None,
                hdfs_file_path=None,
                hdfs_folder=None,
                timestampFormat='yyyy-MM-dd HH:mm:ss',
                dateFormat='yyyy-MM-dd',
                delimeter=',',
                date_cols=[],
                timestamp_cols=[],
                log=log,
                escape='"',
                hdfs_put=True,
                ignoreTrailingWhiteSpace=False,
                schema=None,
                hdfs_delete_local=False):
    "Read from csv file initially on local"
    # https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html
    # The dateFormat field does not parse/assign dates correctly... not sure why.
    # see https://stackoverflow.com/questions/40878243/reading-csv-into-a-spark-dataframe-with-timestamp-and-date-types
    # seems the only way is to assign the schema manually or to cast manually (which is being done here).

    if self.hive_enabled and file_path:
      if not hdfs_folder:
        raise('No value provided for hdfs_folder!')

      hdfs_file_path = hdfs_file_path if hdfs_file_path else self.hdfs_put(
        file_path, hdfs_folder, put=self.hive_enabled, log=log)
      if hdfs_delete_local:
        os.system('rm -rf ' + file_path)
      f_path = hdfs_file_path
    else:
      f_path = file_path

    params = dict(
      path=f_path,
      header=True,
      timestampFormat=timestampFormat,
      # dateFormat=dateFormat,
      inferSchema=False if schema else True,
      schema=schema,
      sep=delimeter,
      escape=escape,
      ignoreTrailingWhiteSpace=ignoreTrailingWhiteSpace,
    )

    if self.version >= 2.2: params['multiLine'] = True
    df = self.spark.read.csv(**params)

    df = self.process_df_fields(df, date_cols, timestamp_cols, dateFormat,
                                timestampFormat)

    log('Finished reading file: ' + f_path)
    return df

  def process_df_fields(self, df, date_cols, timestamp_cols, dateFormat,
                        timestampFormat):
    "Process dataframe fields names and date/time conversions"

    for col in df.columns:
      df = df.withColumnRenamed(col, col.lower().replace(' ', '_'))

    if date_cols:
      date_cols = [c.lower() for c in date_cols]
      df.registerTempTable("df")
      date_str = "to_date(cast(unix_timestamp({c}, '{dateF}') as timestamp)) as {c}"
      sql = '''select {} from df'''.format(', '.join([
        date_str.format(c=col, dateF=dateFormat) if col in date_cols else col
        for col in df.columns
      ]))
      df = self.spark.sql(sql)

    if timestamp_cols:
      timestamp_cols = [c.lower() for c in timestamp_cols]
      df.registerTempTable("df")
      date_str = "cast(unix_timestamp({c}, '{dateF}') as timestamp) as {c}"
      sql = '''select {} from df'''.format(', '.join([
        date_str.format(c=col, dateF=timestampFormat)
        if col in timestamp_cols else col for col in df.columns
      ]))
      df = self.spark.sql(sql)

    return df

  @staticmethod
  def write_csv(df,
                file_path,
                timestampFormat='yyyy-MM-dd HH:mm:ss',
                delimeter=',',
                partitions=1,
                log=log):
    "Write to csv file locally"
    df.repartition(partitions).write.csv(
      file_path + '_',
      mode='overwrite',
      header=True,
      timestampFormat=timestampFormat,
      sep=delimeter,
    )
    os.system(
      'cat {file_path}_/*.csv > {file_path} && rm -rf {file_path}_'.format(
        file_path=file_path))

  def write_csv2(self,
                 df,
                 file_path,
                 hdfs_folder = None,
                 timestampFormat='yyyy-MM-dd HH:mm:ss',
                 dateFormat='yyyy-MM-dd',
                 delimeter=',',
                 partitions=1,
                 log=log,
                 quoteAll=False,
                 gzip=False):
    "Write to csv file correctly from HDFS to local"
    file_name = file_path.split('/')[-1]
    if self.hive_enabled:
      f_path = '{}/{}'.format(hdfs_folder, file_name)
    else:
      f_path = file_path
    df.repartition(partitions).write.csv(
      f_path,
      mode='overwrite',
      header=True,
      timestampFormat=timestampFormat,
      dateFormat=dateFormat,
      sep=delimeter,
      quoteAll=quoteAll)
    if self.hive_enabled:
      os.system('hdfs dfs -getmerge {} {}'.format(f_path, file_path))
      os.system('rm -f .{}.crc'.format(file_path))
    elif partitions > 1:
      # merge into one partition
      os.system('cat {file_path}/*.csv > {file_path}_ && rm -rf {file_path} && mv {file_path}_ {file_path}'.format(file_path=file_path))

    if gzip:
      log('Gzipping ' + file_path)
      os.system('gzip -f ' + file_path)
      file_path = file_path + '.gz'

    log("Wrote data to file {}.".format(file_path))

  @staticmethod
  def write_jsonl(df,
                  file_path,
                  hdfs_folder,
                  mode='overwrite',
                  timestampFormat='yyyy-MM-dd HH:mm:ss',
                  dateFormat='yyyy-MM-dd',
                  partitions=1,
                  log=log):
    "Write to csv file correctly from HDFS to local"

    file_name = file_path.split('/')[-1]

    # Write schema
    schema_file_path = file_path + '.schema'
    schema_data = df.dtypes
    write_jsonl(schema_file_path, schema_data)

    # Write data
    df.repartition(partitions).write.json(
      path='{}/{}'.format(hdfs_folder, file_name),
      mode=mode,
      timestampFormat=timestampFormat,
      dateFormat=dateFormat,
    )
    os.system('hdfs dfs -getmerge {}/{} {}'.format(hdfs_folder, file_name,
                                                   file_path))
    crc_file_path = '{}/.{}.crc'.format('/'.join(file_path.split('/')[:-1]),
                                        file_name)
    os.system('rm -f {}'.format(crc_file_path))

    log("Wrote data to file {}.".format(file_path))

  def convert_fields(self,
                     df,
                     curr_type,
                     new_type,
                     conv_str="cast({col} as {new_type}) as {col}"):
    """Convert fields to specified types"""

    df.registerTempTable("df")
    sql = '''select {} from df'''.format(', '.join([
      conv_str.format(col=col[0], new_type=new_type)
      if col[1] == curr_type else col[0] for col in df.dtypes
    ]))
    df = self.spark.sql(sql)

    return df

  def jdbc_read(self, db_name, sql_table, partition_dict={}, fetchsize=10000, log=log):
    "Read data from table or sub-query SQL"
    db_profile = get_db_profile(db_name)
    null_sql_table = None

    if partition_dict:
      # This attempts the split a range evenly
      # This is not optimal when there is no certainty that the range is
      # continuous, such as an incremented BIGINT primary key.

      if not ('partitionColumn' in partition_dict and 'numPartitions' in partition_dict):
        raise("partition_dict must contain 'partitionColumn' and 'numPartitions'")

      # Automatically obtain lowerBound and upperBound values.
      if not ('lowerBound' in partition_dict and 'upperBound' in partition_dict):
        conn = get_conn(db_name, echo=False)
        sql = conn.template('routine.number_min_max').format(
          field=partition_dict['partitionColumn'],
          table=sql_table,
        )
        log('Getting partitionColumn stats for {}'.format(
          partition_dict['partitionColumn']))
        data = conn.select(sql, echo=False)

        if not len(data):
          raise('Table "{}" seems empty?'.format(sql_table))

        tot_cnt, field_cnt, min_val, max_val = data[0]
        null_cnt = tot_cnt - field_cnt

        # if there are null count, they will be missed
        if null_cnt > 0:
          log('null_cnt ({}) is > 0 for field "{}" in table "{}". Selecting a Null filtered partition!'.format(null_cnt, partition_dict['partitionColumn'], sql_table))

          null_sql_table = "(select * from {} where {} is null)".format(sql_table, partition_dict['partitionColumn'])

          df_null = self.spark.read \
            .format("jdbc") \
            .option("url", db_profile['url']) \
            .option("dbtable", null_sql_table) \
            .option("user", db_profile['user']) \
            .option("password", db_profile['password']) \
            .option("fetchsize", fetchsize) \
            .load()

        partition_dict['lowerBound'] = min_val
        partition_dict['upperBound'] = max_val

      df = self.spark.read \
        .format("jdbc") \
        .option("url", db_profile['url']) \
        .option("dbtable", sql_table) \
        .option("user", db_profile['user']) \
        .option("password", db_profile['password']) \
        .option("partitionColumn", partition_dict['partitionColumn']) \
        .option("lowerBound", partition_dict['lowerBound']) \
        .option("upperBound", partition_dict['upperBound']) \
        .option("numPartitions", partition_dict['numPartitions']) \
        .option("fetchsize", fetchsize) \
        .load()
    else:
      df = self.spark.read \
        .format("jdbc") \
        .option("url", db_profile['url']) \
        .option("dbtable", sql_table) \
        .option("user", db_profile['user']) \
        .option("password", db_profile['password']) \
        .option("fetchsize", fetchsize) \
        .load()
      tot_cnt = df.count()

    if null_sql_table:
      df.registerTempTable("df")
      df_null.registerTempTable("df_null")
      df = self.spark.sql('select * from df union all select * from df_null')

    self._last_df_cnt = tot_cnt
    return df

  def sql(self, *args, **kwargs):
    "Run Spark SQL"
    return self.spark.sql(*args, **kwargs)

  def jdbc_write(self,
                 df,
                 db_profile,
                 sql_table,
                 create_table=True,
                 truncate=False,
                 order_by=[],
                 grant_sql=None,
                 post_sql=None,
                 batch_size=20000,
                 partitions=7,
                 tgt_mode='append',
                 log=log,
                 **kwargs):
    "Write data to table"

    count_recs = get_kw('count_recs', False, kwargs)
    counter = df.count() if count_recs else get_kw('tot_cnt', -1, kwargs)


    if db_profile['type'] == 'oracle':
      # trim field names if is Oracle
      for col in df.columns:
        df = df.withColumnRenamed(col, col[:30]) if len(col) > 30 else df
      for col in df.columns:
        df = df.withColumnRenamed(col, col.upper().replace(' ', '_'))

      # convert booleans to string
      df = self.convert_fields(df, 'boolean', 'string')

    s_t = datetime.datetime.now()
    conn = get_conn(db_profile['name'])

    if truncate and tgt_mode != 'overwrite':
      create_table = False
      conn.execute('TRUNCATE TABLE ' + sql_table)
      time.sleep(2)

    if tgt_mode == 'overwrite':
      field_types = self.df_to_field_types(df)
      conn.create_table(sql_table, field_types, drop=True)
      if grant_sql:
        conn.execute(grant_sql)
      time.sleep(2)
      tgt_mode = 'append'

    partitions = partitions if order_by else 1
    df.repartition(partitions).write \
      .format("jdbc") \
      .option("url", db_profile['url']) \
      .option("dbtable", sql_table) \
      .option("user", db_profile['user']) \
      .option("password", db_profile['password']) \
      .option("batchsize", batch_size) \
      .save(mode=tgt_mode)

    # .option("createTableColumnTypes", create_types_str) \ # 2.2 compatible
    # .option("sessionInitStatement", sessionInitStatement) \ # 2.3 compatible
    # .option("customSchema", customSchema) \ # 2.3 compatible

    # if order_by:
    #   # using many partitions, the data is not ordered
    #   sql = '''
    #   CREATE TABLE {t2} AS SELECT * FROM {t1} ORDER BY {ord};
    #   DROP TABLE {t1} CASCADE CONSTRAINTS PURGE;
    #   ALTER TABLE {t2} RENAME TO {t1n};
    #   '''.format(
    #     t1=sql_table,
    #     # t1n=sql_table.split('.')[-1] if '.' in sql_table else sql_table,
    #     t1n=sql_table.split('.')[-1] if '.' in sql_table else sql_table,
    #     t2=sql_table + 'z',
    #     ord=', '.join(order_by),
    #   )
    #   conn.execute(sql, 'ORDERING', echo=False)

    if post_sql:
      conn.execute(post_sql, 'EXECUTING POST-SQL', echo=False)

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(counter / secs, 1)

    log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
      counter, sql_table, mins, rate))

  def hive_write(self,
                 df,
                 table,
                 mode='overwrite',
                 format='parquet',
                 convert_dec=True,
                 max_partitions=200,
                 show_count=True,
                 tot_cnt=None,
                 order_by=[],
                 log=log):
    "Write to Hive"
    s_t = datetime.datetime.now()

    # Parquet is the default format, which does not play nice with Decimal fields
    # best to convert those to double.
    if convert_dec: df = self.convert_fields(df, 'decimal', 'double')

    log("Loading into hive table {}...".format(table))
    if max_partitions and df.rdd.getNumPartitions() > max_partitions:
      df = df.repartition(max_partitions)

    if format == 'orc':
      df.write.format("orc").saveAsTable(table, mode=mode)
    elif format == 'parquet':
      df.write.saveAsTable(table, mode=mode)

    if format == 'parquet' and order_by:
      log('Ordering table ' + table)

      hive_db = read_yaml(os.getenv('PROFILE_YAML'))['databases']['HIVE']
      h_conn = Beeline(hive_db, echo=False)
      h_conn.convert_to_parquet(table, order_by=order_by)

    secs = (datetime.datetime.now() - s_t).total_seconds()

    mins = round(secs / 60, 1)
    if show_count:
      df.registerTempTable("df")
      counter = self.spark.sql('select count(1) cnt from df').head(1)[0][0]
      if tot_cnt and tot_cnt != counter:
        log('-WARNING: table vs. file count differ! {} != {}'.format(
          counter, tot_cnt))
      rate = round(counter / secs, 1)
      log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
        counter, table, mins, rate))
    else:
      log("Inserted records into table '{}' in {} mins.".format(table, mins))

  def hive_load_large_file(self,
                           file_path,
                           target_table,
                           split_size_GB=5,
                           log=log):
    "Load large files into hive"

    def execute_cmd(cmd):
      rt = os.system(cmd)
      if rt > 0:
        print('ERROR: Return code not 0 !')
        sys.exit(rt)

    file_size = os.path.getsize(file_path)
    file_lines = int(
      os.popen('wc -l "{}"'.format(file_path)).readline().split()[0])
    avg_size_per_line = file_size / file_lines
    lines_split = int(split_size_GB * 1024**3 / avg_size_per_line)

    log('File size is {} MB or {} GB'.format(
      round(1.0 * file_size / (1024**2), 1),
      round(1.0 * file_size / (1024**3), 1),
    ))
    log('Splitting large file...')
    out_folder = file_path + '.split'
    os.system('mkdir -p ' + out_folder)

    # split large file and add headers
    cmd = 'cd {out_folder} && tail -n +2 "{file_path}" | split -l {lines_split} - {output_prefix}'.format(
      out_folder=out_folder,
      file_path=file_path,
      lines_split=lines_split,
      output_prefix=file_path.split('/')[-1] + '.',
    )
    execute_cmd(cmd)

    log('Adding headers...')
    cmd = '''
    cd {out_folder} && for file in *
    do
        head -n 1 {file_path} > tmp_file
        cat $file >> tmp_file
        mv -f tmp_file $file
    done
    '''.format(
      out_folder=out_folder,
      file_path=file_path,
    )
    execute_cmd(cmd)

    files = [out_folder + '/' + fn for fn in os.listdir(out_folder)]

    for i, file_path in enumerate(files):
      log('Loading ' + file_path)
      df1 = self.read_csv2(
        file_path,
        delimeter=',',
        timestampFormat='yyyy-MM-dd HH:mm:ss',
        dateFormat='yyyy-MM-dd',
        date_cols=[])
      self.hive_write(
        df1, target_table, mode='append' if i > 0 else 'overwrite')


class SparkConn(HiveConn):
  "Spark Hive Context Connection"

  df_id = None
  df_ids = []

  def connect(self):
    "Connect / Re-Connect to Database"
    c = struct(self._cred)
    restart = c.restart if 'restart' in c else False
    hive_enabled = c.hive_enabled if 'hive_enabled' in c else False
    master = c.master if 'master' in c else None
    version = c.version if 'version' in c else None
    spark_home = c.spark_home if 'spark_home' in c else None
    self.sparko = Spark(
      restart=restart,
      hive_enabled=hive_enabled,
      master=master,
      spark_home=spark_home,
      version=version)
    self.application_id = self.sparko.sc._jsc.sc().applicationId()

    self._cred.name = self.name = "Spark"
    self.username = c.user

  def get_cursor(self):
    return self.sparko

  def close(self):
    self.sparko.sc.stop()

  def get_cursor_fields(self):
    "Get fields of active Select cursor"

    return self._fields

  def select(self,
             sql,
             rec_name='Record',
             dtype='namedtuple',
             limit=None,
             echo=True,
             log=log):
    "Select from SQL, return list of namedtuples"
    s_t = datetime.datetime.now()

    df1 = self.sparko.sql(sql)
    self.df_id = self.df_id if self.df_id else 'df_' + str(int(time.time()))
    df1.registerTempTable(self.df_id)
    self.df_ids.append(self.df_id)
    df1 = df1.limit(limit) if limit else df1

    if dtype == 'dataframe':
      data = df1.toPandas()
    else:
      data = df1.collect()

    self._fields = df1.columns
    secs = (datetime.datetime.now() - s_t).total_seconds()
    rate = round(len(data) / secs, 1)

    if echo:
      log(" >>> Got {} rows in {} secs [{} r/s].".format(
        len(data), secs, rate))

    return data
