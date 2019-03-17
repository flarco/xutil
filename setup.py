from distutils.core import setup
from setuptools import find_packages

jdbc = ['jaydebeapi']
hive = [
  "thrift",
  "sasl",  # need sasl.h library
  "thrift_sasl",
  "impyla",
]
web = [
  "python-socketio",
  "eventlet",
  "scrapy"
]
aws = [
  "PyAthenaJDBC"
]

version = '0.0.2'

setup(
  name='xutil',
  # packages=['xutil'],
  version=version,
  description='Extra Useful Utilities.',
  author='Fritz Larco',
  author_email='flarco@live.com',
  url='https://github.com/flarco/xutil',
  download_url='https://github.com/flarco/xutil/archive/{}.tar.gz'.format(version),
  keywords=['xutil'],
  packages=find_packages(exclude=['tests']),
  include_package_data=True,
  install_requires=[
    "verboselogs", "coloredlogs", "psutil", "jmespath", "jsonlines",
    "paramiko", "redis", "cx_Oracle", "psycopg2", "pymongo", "requests",
    "pyspark", "hdfs", "sqlalchemy", "prettytable", "pyarrow", "s3fs",
    "sqlparse", "scp"
  ],
  extras_require={
    'jdbc': jdbc,  # needs gcc and g++ to be installed
    'hive': hive,
    'web': web,
    'full': jdbc + hive + web + aws,
  },
  entry_points={
    'console_scripts': [
      'pykill=xutil.cli:pykill',
      'exec-sql=xutil.cli:exec_sql',
      'exec-etl=xutil.cli:exec_etl',
      'xutil-alias=xutil.cli:alias_cli',
      'xutil-create-profile=xutil.cli:create_profile',
      'ipy=xutil.cli:ipy',
      'ipy-spark=xutil.cli:ipy_spark',
    ],
  },
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
