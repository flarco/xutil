from distutils.core import setup
from setuptools import find_packages

jdbc = ['jaydebeapi']
hive = [
  "thrift",
  "sasl", # need sasl.h library
  "thrift_sasl",
]
web = [
  "python-socketio",
  "eventlet",
  "scrapy"
]

setup(
  name='xutil',
  # packages=['xutil'],
  version='0.0.1',
  description='Extra Useful Utilities.',
  author='Fritz Larco',
  author_email='flarco@live.com',
  url='https://github.com/flarco/xutil',
  download_url='https://github.com/flarco/xutil.git',
  keywords=['xutil'],
  packages=find_packages(exclude=['tests']),
  include_package_data=True,
  install_requires=[
    "verboselogs",
    "coloredlogs",
    "psutil",
    "jmespath",
    "jsonlines",
    "paramiko",
    "redis",
    "cx_Oracle",
    "psycopg2",
    "pymongo",
    "requests",
    "pyspark",
    "hdfs",
    "findspark",
    "sqlalchemy",
    "halo",
    "prettytable",
    "pyarrow",
    "s3fs",
    "sqlparse"
  ],
  extras_require={
    'jdbc': jdbc,  # needs gcc and g++ to be installed
    'hive': hive,
    'web': web,
  },
  entry_points={
    'console_scripts': [
      'pykill=xutil.cli:pykill',
      'exec-sql=xutil.cli:exec_sql',
      'exec-etl=xutil.cli:exec_etl',
      'xutil-alias=xutil.cli:alias_cli',
    ],
  },
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: Windows', 'Operating System :: MacOS',
    'Operating System :: Unix', 'Topic :: Utilities'
  ])
