#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

import botocore
import boto3
import threading, logging, time
import random
import pickle
import json
import numpy as np
import redis
from kafka import KafkaProducer


# edge_count = 0
# total_wedges = 0
# edge_res = []
# wedge_res = []
# is_closed = []


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
    if edge1[0] == edge2[0] and edge1[1] != edge2[1]:
        return True
    if edge1[0] == edge2[1] and edge1[1] != edge2[0]:
        return True
    if edge1[1] == edge2[1] and edge1[0] != edge2[0]:
        return True
    if edge1[1] == edge2[0] and edge1[0] != edge2[1]:
        return True
    return False


def update(redis_db, new_edge):

    edge_count = int(redis_db.get('edge_count'))
    total_wedges = int(redis_db.get('total_wedges'))

    edge_count += 1
    print("EDGE COUNT: " + str(edge_count))

    edge_res = pickle.loads(redis_db.get('edge_res'))
    wedge_res = pickle.loads(redis_db.get('wedge_res'))
    is_closed = pickle.loads(redis_db.get('is_closed'))
    updated_edge_res = False

    print(edge_res)
    print(wedge_res)
    print(is_closed)

    for i in range(len(wedge_res)):
        if is_closed_by(wedge_res[i], new_edge):
            is_closed[i] = True
    for i in range(len(edge_res)):
        x = random.uniform(0, 1)
        if x < (1 / float(edge_count)):
            edge_res[i] = new_edge
            updated_edge_res = True
    if updated_edge_res:
        new_wedges = []
        for i in range(len(edge_res)):
            if creates_wedge(edge_res[i], new_edge):
                new_wedges.append(get_wedge(edge_res[i], new_edge))
        total_wedges += len(new_wedges)
        for i in range(len(wedge_res)):
            x = random.uniform(0, 1)
            if total_wedges > 0 and x < (len(new_wedges) / float(total_wedges)):
                w = random.choice(new_wedges)
                wedge_res[i] = w
                is_closed[i] = False

    print("TOTAL WEDGES COUNT: " + str(total_wedges))
    pickled_edge_res = pickle.dumps(edge_res)
    redis_db.set('edge_res', pickled_edge_res)
    pickled_wedge_res = pickle.dumps(wedge_res)
    redis_db.set('wedge_res', pickled_wedge_res)
    pickled_is_closed = pickle.dumps(is_closed)
    redis_db.set('is_closed', pickled_is_closed)

    redis_db.incr('edge_count')
    redis_db.set('total_wedges', total_wedges)

    # print(edge_res)
    # print(wedge_res)
    # print(is_closed)

    return total_wedges, (np.sum(is_closed)/float(len(is_closed)))


def streaming_triangles(redis_db, new_edge):
    k = update(redis_db, new_edge)
    # print(tot_wedges)
    transitivity = 3*k
    redis_db.set('transitivity', transitivity)
    print(transitivity)
    # redis_db.set('total_wedges', redis_db.get('tot_wedges'))


# Extract relevant data from json body
def extract_edge(json_obj):
    json_data = json.loads(json_obj)

    # Sender data
    from_id = int(json_data['actor']['id'])

    # Receiver data
    to_id = int(json_data['transactions'][0]['target']['id'])

    return tuple((from_id, to_id))


class Producer(threading.Thread):
    daemon = True

    # def __init__(self):
    #   super(Producer, self).__init__()
    #   self.producer = KafkaProducer(bootstrap_servers='52.25.139.222:9092')


    def run(self):
        producer = KafkaProducer(bootstrap_servers='52.25.139.222:9092')
        # producer = KafkaProducer(bootstrap_servers='localhost:9092')

        # Set up Redis connection
        redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com'
        # redis_server = 'localhost'
        redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

        bucket_name = 'venmo-json'
        bucket = self.__get_s3_bucket__(bucket_name)
        # for obj in bucket.objects.filter(Prefix='2017_02'):
        #     data = obj.get()['Body']
        #     json_body = data.read().splitlines()
        #     for json_obj in json_body:
        #         producer.send('Venmo-Transactions-Dev', json_obj)
        #         time.sleep(0.01)
        #         print json_obj + '\n' + '==============================================================================' + '\n'

        for obj in bucket.objects.limit(100):
            obj_body = obj.get()['Body']
            json_body = obj_body.read().splitlines()
            for json_obj in json_body:
                # producer.send('Venmo-Transactions-Dev', json_obj)
                new_edge = extract_edge(json_obj)
                streaming_triangles(redis_db, new_edge)
                time.sleep(0.5)
                print json_obj + '\n' + '==============================================================================' + '\n'

    def __get_s3_bucket__(self, bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
                print (e.response['404 Error: bucket not found'])
            else:
                print (e.response['Error'])
            return None
        else:
            return s3.Bucket(bucket_name)



if __name__ == "__main__":
    print "Entered main"
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    producer = Producer()
    producer.start()
    time.sleep(1200)