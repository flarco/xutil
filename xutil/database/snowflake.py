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

  def connect(self):
    "Connect / Re-Connect to Database"

    import snowflake.connector

    cred = struct(self._cred) if isinstance(self._cred, dict) else None

    self.connection = snowflake.connector.connect(
      user=cred.user,
      password=cred.password,
      account=cred.host,
      session_parameters={}
    )
    self.cursor = None
    self.name = 'snowflake'
    self.username = cred.user if cred else ''

    self.get_cursor()

  def get_dialect(self, echo=False):
    """SQLAlchemy dialect"""
    from sqlalchemy.dialects import postgresql
    return postgresql

  
  def create_engine(self, conn_str=None, echo=False):
    import sqlalchemy

    cred = struct(self._cred)
    conn_str = 'snowflake://{user}:{password}@{account}/'.format(
        user=cred.user,
        password=cred.password,
        account=cred.host,
    )

    self.engine = sqlalchemy.create_engine(conn_str, echo=echo)

    return self.engine