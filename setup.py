from distutils.core import setup
from setuptools import find_packages
import os

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
  "scrapy",
  "Flask-SSLify"
]
aws = [
  "PyAthenaJDBC"
]
spark = [
  "pyspark"
]

version = '0.2.1'

setup(
  name='xutil',
  # packages=['xutil'],
  version=version,
  description='Extra Useful Utilities.',
  author='Fritz Larco',
  author_email='flarco@live.com',
  url='https://github.com/flarco/xutil',
  download_url='https://github.com/flarco/xutil/archive/master.zip',
  keywords=['xutil'],
  packages=find_packages(exclude=['tests']),
  include_package_data=True,
  test_suite='xutil.tests',
  long_description=open(os.path.join(os.path.dirname(__file__),
                                     'README.rst')).read(),
  install_requires=[
    "verboselogs==1.7",
    "coloredlogs==10.0",
    "psutil==5.6.3",
    "jmespath==0.9.4",
    "jsonlines==1.2.0",
    "paramiko==2.7.1",
    "redis==3.4.1",
    "cx_Oracle==7.3.0",
    "psycopg2-binary==2.8.4",
    "pymongo==3.10.1",
    "requests==2.22.0",
    "hdfs==2.5.8",
    "SQLAlchemy==1.3.9",
    "prettytable==0.7.2",
    "s3fs==0.4.0",
    "sqlparse==0.3.0",
    "scp==0.13.2",
    "ruamel.yaml==0.16.7",
    "pyarrow",
    "numpy",
    "pandas",
    "recordclass==0.13.1",
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
      'xutil-profile-entry=xutil.cli:profile_entry',
      'ipy-spark=xutil.cli:ipy_spark',
    ],
  },
  classifiers=[
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: MacOS', 'Operating System :: Unix',
    'Topic :: Utilities'
  ])
