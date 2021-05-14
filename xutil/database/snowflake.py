from xutil.database.base import DBConn
from xutil.helpers import struct

class SnowflakeConn(DBConn):
  "Snowflake Connection"

  data_map = dict(
    string='varchar',
    integer='integer',
    decimal='decimal()',
    date='date',
    datetime='timestamp',
    text='text',
  )

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import postgresql
    return postgresql

  def set_variables(self):
    self.connection.autocommit = False
    self.connection.execute('commit')
  
  def create_engine(self, conn_str=None, echo=False):
    import sqlalchemy

    cred = struct(self._cred)
    args = [
      'snowflake://',
      cred.user,
      ':',
      cred.password,
      '@',
      cred.account,
      '/',
      cred.database,
      '/',
      cred.schema,
      '?warehouse=',
      cred.warehouse,
      '&role=',
      cred.role,
    ]
    conn_str = ''.join(args)
    self.engine = sqlalchemy.create_engine(conn_str, echo=echo)

    return self.engine