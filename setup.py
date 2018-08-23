from distutils.core import setup

setup(
  name='xutil',
  packages=['xutil'],
  version='0.0.1',
  description='Extra Useful Utilities.',
  author='Fritz Larco',
  author_email='flarco@live.com',
  url='https://github.com/flarco/xutil',
  download_url='https://github.com/sheriferson/simplestatistics/tarball/0.2.5',
  keywords=['xutil'],
  install_requires=[
    "jmespath",
    "jsonlines",
    "paramiko",
    "redis",
    "cx_Oracle",
    "pymongo",
    "jaydebeapi",
    "requests",
    "pyspark",
    "hdfs",
    "findspark",
    "sqlalchemy",

    # web
    "python-socketio",

    # pyhive
    "thrift",
    # "sasl", # need sasl.h library
    "thrift_sasl",
  ],
  classifiers=[
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3', 'Intended Audience :: Developers',
    'Intended Audience :: Education', 'Intended Audience :: Science/Research',
    'Operating System :: Windows', 'Operating System :: MacOS',
    'Operating System :: Unix', 'Topic :: Utilities'
  ])
