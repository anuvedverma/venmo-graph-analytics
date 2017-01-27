import redis

# REDIS_SERVER = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com'
REDIS_SERVER = 'localhost'
r = redis.StrictRedis(host=REDIS_SERVER, port=6379, db=0)
# r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('foo', 'bar')
print r.get('foo')