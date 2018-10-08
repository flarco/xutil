# xutil

This will be a Python package containing all the utility functions and libraries that are commonly used.

http://sherifsoliman.com/2016/09/30/Python-package-with-GitHub-PyPI/

This link displayed how to do this back with Youtube.

# Install

```
pip install -U git+git://github.com/flarco/xutil.git
```

# Database

Why not use SQLAlchemy (SA)? http://docs.sqlalchemy.org/en/latest/faq/performance.html#i-m-inserting-400-000-rows-with-the-orm-and-it-s-really-slow

It has been demontrated the SA is not performant when it comes to speedy ETL.

## SQL Server

### Installation

Make sure ODBC is installed.

```
brew install unixodbc
apt-get install unixodbc
```

Then, install the drivers

https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-2017

```
odbcinst -j
```

## Spark SQL

It is the user's responsibility to properly set up the SPARK_HOME environment and configurations.
This library uses pyspark and will default to the SPARK_HOME settings.

# Useful config.py

https://github.com/apache/incubator-airflow/blob/master/setup.py

https://github.com/dask/dask/blob/master/setup.py

https://github.com/tartley/colorama/blob/master/setup.py

# Alias

Installing aliases and adding it to your `.bash_profile`

```
xutil-alias
```

# Dev

```
pip install -e /path/to/xutil
```

# Todo

- Use Spark JDBC? For what?

  - ETL:
    - db to db: need to stream from source instead of holding in memory (which Sparkd does). Holding in memory large tables fails. Best to stream.
      - for RDBMS to hive: stream to multi csvs. gzip them and move them to HDFS. load into spark. load into Hive.
      - for RDBMS to RDBMS: use Python native ? or load into multi CSVs and bulk load...
      - for HIVE to RDBMS: take advatange of HDFS and use Spark SQL to JDBC into table in parallel.
    - db to ff: stream to multi csvs
    - ff to db: load into multi CSVs and bulk load...
    - csv split

- build database tests for:

  - select
  - execute
  - execute_multi
  - stream
  - all analysis

- Add DB functions:

  - insert
  - upsert (update or insert)

* Finish DB libs:
  -X Spark
  -X SQLServer
  - SQlite
    -X JDBC (ability to use SQL server from Linux)
    -X Spark JDBC
