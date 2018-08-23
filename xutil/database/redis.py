from multiprocessing import Process

import redis, json

from xutil.helpers import jdumps, log


class RedisConn:
  def __init__(self, host, port, db, hkey=None):
    self.conn = redis.StrictRedis(host=host, port=port, db=db)
    self.hkey = hkey
    self.new_pipeline()
    self.pubsub = self.conn.pubsub()
  
  def new_pipeline(self):
    self.pipe = self.conn.pipeline()
    return self.pipe
  
  def set(self, key, val, pipe=False):
    "Set val for key"
    if pipe:
      return self.pipe.set(key, val)
    else:
      return self.conn.set(key, val)
  
  def get(self, key):
    "Get val of key"
    val = self.conn.get(key)
    return val.decode() if val else val
  
  def delete(self, key):
    "Delete key"
    return self.conn.delete(key)
  
  def append(self, key, val, pipe=False):
    "Append val on key"
    if pipe:
      return self.pipe.append(key, val)
    else:
      return self.conn.append(key, val)
  
  def setj(self, key, obj, pipe=False):
    "Set object as JSON string for key"
    if pipe:
      return self.pipe.set(key, jdumps(obj))
    else:
      return self.conn.set(key, jdumps(obj))
  
  def getj(self, key):
    "Get object from JSON string of key"
    val = self.conn.get(key)
    return json.loads(val.decode()) if val else val
  
  def hset(self, key, val, hkey=None, pipe=False):
    "Set val for key of hash"
    hkey = hkey if hkey else self.hkey
    if pipe:
      return self.pipe.hset(hkey, key, val)
    else:
      return self.conn.hset(hkey, key, val)
  
  def hget(self, key, hkey=None):
    "Get val of key of hash"
    hkey = hkey if hkey else self.hkey
    val = self.conn.hget(hkey, key)
    return val.decode() if val else val
  
  def hdel(self, hkey, *names):
    "Delete key in hash"
    hkey = hkey if hkey else self.hkey
    return self.conn.hdel(hkey, *names)
  
  def hgetall(self, hkey):
    "Get all key/val of hash"
    val_dict = self.conn.hgetall(hkey)
    val_dict = {k.decode(): val_dict[k].decode() for k in val_dict} if val_dict else val_dict
    return val_dict
  
  def hsetj(self, key, obj, hkey=None, pipe=False):
    "Set object as JSON string for key of hash"
    hkey = hkey if hkey else self.hkey
    if pipe:
      return self.pipe.hset(hkey, key, jdumps(obj))
    else:
      return self.conn.hset(hkey, key, jdumps(obj))
  
  def hgetj(self, key, hkey=None):
    "Get object from JSON string of key of hash"
    hkey = hkey if hkey else self.hkey
    val = self.conn.hget(hkey, key)
    return json.loads(val.decode()) if val else val
  
  def publish(self, channel, payload):
    "Publish a message to a channel"
    payload = jdumps(payload)
    self.conn.publish(channel, payload)
  
  def subscribe(self, channel):
    "Subscribe to a channel"
    self.pubsub.subscribe(channel)
    return self.pubsub
  
  def rpush(self, name, *values):
    "RPUSH to List"
    return self.conn.rpush(name, *values)
  
  def lpush(self, name, *values):
    "LPUSH to List"
    return self.conn.lpush(name, *values)
  
  def rpop(self, name):
    "RPUSH to List"
    return self.conn.rpop(name)
  
  def lpop(self, name):
    "LPUSH to List"
    return self.conn.lpop(name)
  
  def llen(self, name):
    "LLEN of List: return length of list"
    return self.conn.llen(name)
  
  def ltrim(self, name, start, end):
    "LTRIM of List: Trim the list ``name``, removing all values not within the slice between ``start`` and ``end``"
    return self.conn.ltrim(name, start, end)
  
  def lrange(self, name, start=0, end=-1):
    "LRANGE of List"
    return self.conn.lrange(name, start, end)
  
  def lrangej(self, name, start=0, end=-1):
    "LRANGE of List"
    return [json.loads(obj.decode()) if obj else obj for obj in self.conn.lrange(name, start, end)]
  
  def lset(self, name, index, value):
    "LSET index of List"
    return self.conn.lset(name, index, value)
  
  def lsetj(self, name, index, obj):
    "LSET index of List"
    return self.conn.lset(name, index, jdumps(obj))
  
  def lrem(self, name, count, value):
    "LREM index of List"
    return self.conn.lset(name, count, value)
  
  def lindex(self, name, index):
    "LINDEX return value of index in List"
    return self.conn.lindex(name, index)
  
  def lindexj(self, name, index):
    "LINDEX return value of index in List"
    obj = self.conn.lindex(name, index)
    return json.loads(obj.decode()) if obj else obj
  
  def rpoplpush(self, src, dst):
    "RPOP a value off of the ``src`` list and atomically LPUSH it on to the ``dst`` list.  Returns the value."
    return self.conn.rpoplpush(src, dst)
  
  def listen(self, channel=None):
    "Listen to subscribed channel, yield data"
    if channel: self.pubsub.subscribe(channel)
    
    init_val = False
    try:
      for payload in self.pubsub.listen():
        try:
          payload_data = json.loads(payload['data'].decode('utf-8'))
          yield payload_data
        except AttributeError:
          pass
    except (KeyboardInterrupt, SystemExit):
      return
  
  def start_listener(self, channel, queue, echo=True):
    log('Running Listener on channel: ' + channel, color='green')
    
    def run_redis_listener(redb, channel, queue):
      for payload in redb.listen(channel):
        queue.put(payload)
        if payload == 'exit': return
    
    self.listener_proc = Process(target=run_redis_listener, args=(self, channel, queue))
    self.listener_proc.start()
    return self.listener_proc
  
  def stop_listener(self, channel):
    log('Stopping listener on channel: ' + channel)
    self.conn.publish(channel, jdumps('exit'))
