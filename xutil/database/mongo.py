import datetime

from xutil.database.base import DBConn
from xutil.helpers import struct, log


class MongoDBConn(DBConn):
  "MongoDB connection"

  def connect(self):
    "Connect / Re-Connect to Database"
    import pymongo
    c = struct(self._cred)

    self.conn = pymongo.MongoClient(
      'mongodb://{host}:{port}/{database}'.format(
        host=c.host,
        port=c.port,
        database=c.database,
      ))

    self._cred['user'] = ''

  def get_coll(self, table):
    db, coll_name = table.split('.') if '.' in table else ('local', table)
    return self.conn[db][coll_name]

  def create_index(self, table, fields):
    "Create index on collection"
    db, coll_name = self.get_coll(table)
    coll_name.create_index('id', unique=True)

  def select_table(self, table, where_dict={}, echo=True, log=log):
    "Select a collection, return list of dicts"
    s_t = datetime.datetime.now()

    collection = self.get_coll(table)
    data = [r for r in collection.find(where_dict)]

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(len(data) / secs, 1)
    if echo:
      log(" >>> Got {} rows in {} secs [{} r/s].".format(
        len(data), secs, rate))

    return data

  def select(self, table, where_dict={}, echo=True, log=log):
    return self.select_table(table, where_dict, echo)

  def insert(self, table, data, echo=True):
    "Inserts list of dics"
    s_t = datetime.datetime.now()

    collection = self.get_coll(table)
    # inserted_ids = collection.insert_many(data).inserted_ids
    collection.insert_many(data)

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(len(data) / secs, 1)
    if echo:
      log("Inserted {} records into table '{}' in {} mins [{} r/s].".format(
        len(data), table, mins, rate))

    return len(data)

  def replace(self,
              table,
              data,
              pk_filter_dict,
              field_types=None,
              commit=True,
              echo=True):
    "Replace list of dics; pk_filter_dict needs to be condition dict"
    # http://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write

    from pymongo import InsertOne, DeleteOne, ReplaceOne
    s_t = datetime.datetime.now()

    collection = self.get_coll(table)

    requests = [
      ReplaceOne(pk_filter_dict, row_dict, upsert=True) for row_dict in data
    ]
    result = collection.bulk_write(requests)

    secs = (datetime.datetime.now() - s_t).total_seconds()
    mins = round(secs / 60, 1)
    rate = round(len(data) / secs, 1)
    if echo:
      log("Replaced {} records into table '{}' in {} mins [{} r/s].".format(
        len(data), table, mins, rate))

    return len(data)
