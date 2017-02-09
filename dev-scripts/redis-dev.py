import redis
import pickle
import random
import numpy as np


def streaming_triangles(redis_db, edge_res_size, wedge_res_size):

    edge_res = [list(tuple((0, 0))) for _ in xrange(edge_res_size)]
    pickled_edge_res = pickle.dumps(edge_res)
    redis_db.set('edge_res', pickled_edge_res)

    wedge_res = [list(tuple((0, 0, 0))) for _ in xrange(wedge_res_size)]
    pickled_wedge_res = pickle.dumps(wedge_res)
    redis_db.set('wedge_res', pickled_wedge_res)

    is_closed = [False for _ in xrange(wedge_res_size)]
    pickled_is_closed = pickle.dumps(is_closed)
    redis_db.set('is_closed', pickled_is_closed)

    tot_wedges = 0
    count = 0
    # edges = [(2, 1), (2, 3), (3, 4), (3, 1), (4, 2), (5, 2), (4, 5), (5, 1), (5, 6), (1, 7), (8, 3), (9, 4)]
    edges = [(2, 1), (1, 3), (1, 4), (5, 1), (6, 1), (1, 7), (7, 2), (2, 3), (4, 5)]
    random.shuffle(edges)
    for new_edge in edges:
        count += 1
        (tot_wedges, k) = update(redis_db, new_edge, count, tot_wedges)
    print(tot_wedges)
    print(3*k)

# STREAMING-TRIANGLES
# 1 Initialize edge_res and wedge_res
# 2 For each edge new_edge in stream:
# 3 Call UPDATE(new_edge).
# 4 Let p be the fraction of entries in isClosed set to true
# 5 Set k = 3p

# UPDATE
# 1 for i = 1...sw:
# 2     if wedge_res[i] closed by new_edge:
# 3     isClosed[i] = true
# 4 for i = 1...se:
# 5     x = random(0, 1)
# 6     if x < 1/t
# 7         edge_res[i] = new_edge
# 8 if there were any updates of edge_res:
# 9     Update tot_wedges, the number of wedges formed by edge_res
# 10    Determine Nt (wedges involving et) and let new_wedges = |Nt|
# 11    for i...sw:
# 12        Pick a random number x in [0, 1]
# 13        if x< (new wedges/tot wedges):
# 14            w = uniform random wedge in Nt
# 15            wedge res[i] = w
# 16            isClosed[i] = false


def update(redis_db, new_edge, count, tot_wedges):
    edge_res = pickle.loads(redis_db.get('edge_res'))
    wedge_res = pickle.loads(redis_db.get('wedge_res'))
    is_closed = pickle.loads(redis_db.get('is_closed'))
    updated_edge_res = False
    # print(edge_res)
    # print(wedge_res)
    # print(is_closed)

    for i in range(len(wedge_res)):
        if is_closed_by(wedge_res[i], new_edge):
            is_closed[i] = True
    for i in range(len(edge_res)):
        x = random.uniform(0, 1)
        if x < (1/float(count)):
            edge_res[i] = new_edge
            updated_edge_res = True
    if updated_edge_res:
        new_wedges = []
        for i in range(len(edge_res)):
            if creates_wedge(edge_res[i], new_edge):
                new_wedges.append(get_wedge(edge_res[i], new_edge))
        tot_wedges += len(new_wedges)
        for i in range(len(wedge_res)):
            x = random.uniform(0, 1)
            if tot_wedges > 0 and x < (len(new_wedges) / float(tot_wedges)):
                w = random.choice(new_wedges)
                wedge_res[i] = w
                is_closed[i] = False

    pickled_edge_res = pickle.dumps(edge_res)
    redis_db.set('edge_res', pickled_edge_res)
    pickled_wedge_res = pickle.dumps(wedge_res)
    redis_db.set('wedge_res', pickled_wedge_res)
    pickled_is_closed = pickle.dumps(is_closed)
    redis_db.set('is_closed', pickled_is_closed)

    # print(edge_res)
    # print(wedge_res)
    # print(is_closed)

    return tot_wedges, (np.sum(is_closed)/float(len(is_closed)))


def get_wedge(edge1, edge2):
    if edge1[0] == edge2[0]:
        return tuple((edge2[1], edge1[0], edge1[1]))
    if edge1[0] == edge2[1]:
        return tuple((edge2[0], edge1[0], edge1[1]))
    if edge1[1] == edge2[0]:
        return tuple((edge2[1], edge1[1], edge1[0]))
    if edge1[1] == edge2[1]:
        return tuple((edge2[0], edge1[1], edge1[0]))
    return None

def is_closed_by(wedge, edge):
    if (wedge[0] == edge[0] and wedge[2] == edge[1]) or (wedge[0] == edge[1] and wedge[2] == edge[0]):
        return True
    return False


def creates_wedge(edge1, edge2):
    # print("CHECKING IF " + str(edge1) + " AND " + str(edge2) + " CREATE A WEDGE")
    if edge1[0] == edge2[0] and edge1[1] != edge2[1]:
        return True
    if edge1[0] == edge2[1] and edge1[1] != edge2[0]:
        return True
    if edge1[1] == edge2[1] and edge1[0] != edge2[0]:
        return True
    if edge1[1] == edge2[0] and edge1[0] != edge2[1]:
        return True
    return False


if __name__ == "__main__":

    # Set up Redis connection
    redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com'
    # redis_server = 'localhost'
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    # Start algorithm
    # redis_db.delete('edge_res')
    # redis_db.delete('wedge_res')
    # redis_db.delete('is_closed')
    # streaming_triangles(redis_db, 20000, 20000)

# r.set('foo', 'bar')
# print r.get('foo')