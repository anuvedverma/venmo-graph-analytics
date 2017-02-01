import redis

redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com'
# redis_server = 'localhost'
r = redis.StrictRedis(host=redis_server, port=6379, db=0)
# r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('foo', 'bar')
print r.get('foo')