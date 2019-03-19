# xutil

This is a Python package containing all the utility functions and libraries that are commonly used.

# Install

```
pip install xutil
pip install xutil[jdbc] # for JDBC connectivity. Requires JPype1.
pip install xutil[web] # for web scraping. Requires Twisted.
pip install xutil[hive] # for Hive connectivity. Requires SASL libraries.
```

## Windows

If you face the message 'error: Microsoft Visual C++ 14.0 is required. Get it with "Microsoft Visual C++ Build Tools"', you can quickly install the build tools with chocolatey (https://chocolatey.org/)

```
choco install -y VisualCppBuildTools
```

# CLI

Available commands:

```bash
xutil-alias          # add useful alias commands, see xutil/alias.sh
xutil-create-profile # creates ~/profile.yaml from template.
exec-etl --help      # Execute various ETL operations.
exec-sql --help      # Execute SQL from command line
ipy                  # launch ipython with pre-defined modules/functions imported
ipy-spark --help     # launch ipython Spark with pre-defined modules/functions imported
pykill pattern       # will swiftly kill any process with the command string mathing pattern
```

# Databases

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


# Dev

```
pip install -e /path/to/xutil
```

## Release

- Update version in [setup.py](./setup.py).
- Draft new release on Github: <https://github.com/flarco/xutil/releases/new>

```
git clone https://github.com/flarco/xutil.git
cd xutil
m2r --overwrite README.md
python setup.py sdist && twine upload --skip-existing dist/*
```
