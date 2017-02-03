from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
import boto3
import redis
import pickle
import numpy as np
import random
import json


EDGE_RES_SIZE = 2000
WEDGE_RES_SIZE = 2000
COUNT = 0
TOT_WEDGES = 0

# Extract relevant data from json body
def extract_data(json_body):

    # Sender data
    from_id = json_body['actor']['id']
    from_firstname = json_body['actor']['firstname']
    from_lastname = json_body['actor']['lastname']
    from_username = json_body['actor']['username']
    from_picture = json_body['actor']['picture']

    # Receiver data
    to_id = json_body['transactions'][0]['target']['id']
    to_firstname = json_body['transactions'][0]['target']['firstname']
    to_lastname = json_body['transactions'][0]['target']['lastname']
    to_username = json_body['transactions'][0]['target']['username']
    to_picture = json_body['transactions'][0]['target']['picture']

    # Transaction data
    message = json_body['message']
    timestamp = json_body['created_time']

    # Filter out invalid values
    if not from_username:
        from_username = 'N/A'
    if not to_username:
        to_username = 'N/A'

    if not from_firstname:
        from_firstname = 'N/A'
    if not to_firstname:
        to_firstname = 'N/A'

    if not from_lastname:
        from_lastname = 'N/A'
    if not to_lastname:
        to_lastname = 'N/A'

    # Output data dictionary
    data = {'from_id': int(from_id),
            'from_firstname': from_firstname,
            'from_lastname': from_lastname,
            'from_username': from_username,
            'from_picture': from_picture,
            'to_id': int(to_id),
            'to_firstname': to_firstname,
            'to_lastname': to_lastname,
            'to_username': to_username,
            'to_picture': to_picture,
            'message': message,
            'timestamp': timestamp}
    return data


def analyze_message(message, neighbor):
    color_map = {}
    # Insert real analysis here
    if random.uniform(0, 1) < 0.75:
        color_map['red'] = neighbor
    else: color_map['red'] = None

    if random.uniform(0, 1) < 0.25:
        color_map['blue'] = neighbor
    else: color_map['blue'] = None

    if random.uniform(0, 1) < 0.5:
        color_map['yellow'] = neighbor
    else: color_map['yellow'] = None

    if random.uniform(0, 1) < 0.1:
        color_map['green'] = neighbor
    else: color_map['green'] = None

    if random.uniform(0, 1) < 0.33:
        color_map['black'] = neighbor
    else: color_map['black'] = None


    # Update color_map based on analysis results
    output = {'red': color_map['red'],
              'blue': color_map['blue'],
              'yellow': color_map['yellow'],
              'green': color_map['green'],
              'black': color_map['black']}
    return output


# Create an item
def create_item(table, data):
    table.put_item(
       Item={
           'id': data['id'],
           'username': data['username'],
           'firstname': data['firstname'],
           'lastname': data['lastname'],
           'picture': data['picture'],
           'red_neighbors': [data['red_neighbor']],
           'blue_neighbors': [data['blue_neighbor']],
           'yellow_neighbors': [data['yellow_neighbor']],
           'green_neighbors': [data['green_neighbor']],
           'black_neighbors': [data['black_neighbor']],
           # 'red': int(data['color'] == 'red'),
           # 'blue': int(data['color'] == 'blue'),
           # 'yellow': int(data['color'] == 'yellow'),
           # 'green': int(data['color'] == 'green'),
           # 'black': int(data['color'] == 'black'),
           'num_transactions': 1
       }
    )


# Update an existing item
def update_item(table, data):
    table.update_item(
        Key={
            'id': data['id']
        },
        UpdateExpression='SET num_transactions = num_transactions + :inc,' +
                            # data['color'] + ' = ' + data['color'] + ' + :inc,' +
                         '''username = :username,
                            firstname = :firstname,
                            lastname = :lastname,
                            picture = :picture,
                            red_neighbors = list_append(red_neighbors, :red_neighbor),
                            blue_neighbors = list_append(blue_neighbors, :blue_neighbor),
                            yellow_neighbors = list_append(yellow_neighbors, :yellow_neighbor),
                            green_neighbors = list_append(green_neighbors, :green_neighbor),
                            black_neighbors = list_append(black_neighbors, :black_neighbor)''',

        ExpressionAttributeValues={
            ':username': data['username'],
            ':firstname': data['firstname'],
            ':lastname': data['lastname'],
            ':picture': data['picture'],
            ':red_neighbor': [data['red_neighbor']],
            ':blue_neighbor': [data['blue_neighbor']],
            ':yellow_neighbor': [data['yellow_neighbor']],
            ':green_neighbor': [data['green_neighbor']],
            ':black_neighbor': [data['black_neighbor']],
            ':inc': 1
        }
    )


# Update DynamoDB according to new transaction data
def update_dynamodb(table, data_dict):
    # Sender data
    message = data_dict['message']
    color_map = analyze_message(message, data_dict['to_id'])
    sender_data = {'id': data_dict['from_id'],
                   'username': data_dict['from_username'],
                   'firstname': data_dict['from_firstname'],
                   'lastname': data_dict['from_lastname'],
                   'picture': data_dict['from_picture'],
                   'red_neighbor': color_map['red'],
                   'blue_neighbor': color_map['blue'],
                   'yellow_neighbor': color_map['yellow'],
                   'green_neighbor': color_map['green'],
                   'black_neighbor': color_map['black']}
    try:
        update_item(table, sender_data)
    except ClientError as e:
        print(e.response['Error']['Message'])
        # print(e.response)
        create_item(table, sender_data)

    # Receiver data
    message = data_dict['message']
    color_map = analyze_message(message, data_dict['from_id'])
    receiver_data = {'id': data_dict['to_id'],
                     'username': data_dict['to_username'],
                     'firstname': data_dict['to_firstname'],
                     'lastname': data_dict['to_lastname'],
                     'picture': data_dict['to_picture'],
                     'red_neighbor': color_map['red'],
                     'blue_neighbor': color_map['blue'],
                     'yellow_neighbor': color_map['yellow'],
                     'green_neighbor': color_map['green'],
                     'black_neighbor': color_map['black']}
    try:
        update_item(table, receiver_data)
    except ClientError as e:
        print(e.response['Error']['Message'])
        # print(e.response)
        create_item(table, receiver_data)


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
    global EDGE_RES_SIZE
    global WEDGE_RES_SIZE
    global COUNT
    global TOT_WEDGES

    COUNT += 1

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
        if x < (1 / float(COUNT)):
            edge_res[i] = new_edge
            updated_edge_res = True
    if updated_edge_res:
        new_wedges = []
        for i in range(len(edge_res)):
            if creates_wedge(edge_res[i], new_edge):
                new_wedges.append(get_wedge(edge_res[i], new_edge))
        TOT_WEDGES += len(new_wedges)
        for i in range(len(wedge_res)):
            x = random.uniform(0, 1)
            if TOT_WEDGES > 0 and x < (len(new_wedges) / float(TOT_WEDGES)):
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

    return np.sum(is_closed) / float(len(is_closed))


def streaming_triangles(redis_db, new_edge):
    k = update(redis_db, new_edge)
    # print(tot_wedges)
    transitivity = 3*k
    redis_db.set('transitivity', transitivity)


# Send data to DynamoDB/Redis databases
def send_partition(iter):
    # DynomoDB connection
    dynamodb = boto3.resource('dynamodb',
                            aws_access_key_id='dummy-access-id',
                            aws_secret_access_key='dummy-secret-access-key',
                            region_name='us-west-2',
                            endpoint_url='http://localhost:8000')  # Set DynamoDB connection (local)
    # dynamodb = boto3.resource('dynamodb') # Set DynamoDB connection (cluster)
    dynamo_table = dynamodb.Table('venmo-graph-analytics-dev')  # Set DynamoDB table

    # Redis connection
    redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com' # Set Redis connection (local)
    # redis_server = 'localhost' # Set Redis connection (cluster)
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    # Init edge/wedge reservoirs
    edge_res_size = 2000
    wedge_res_size = 2000

    global EDGE_RES_SIZE
    global WEDGE_RES_SIZE
    edge_res = [list(tuple((0, 0))) for _ in xrange(EDGE_RES_SIZE)]
    pickled_edge_res = pickle.dumps(edge_res)
    redis_db.set('edge_res', pickled_edge_res)

    wedge_res = [list(tuple((0, 0, 0))) for _ in xrange(WEDGE_RES_SIZE)]
    pickled_wedge_res = pickle.dumps(wedge_res)
    redis_db.set('wedge_res', pickled_wedge_res)

    is_closed = [False for _ in xrange(WEDGE_RES_SIZE)]
    pickled_is_closed = pickle.dumps(is_closed)
    redis_db.set('is_closed', pickled_is_closed)

    # Route stream data to appropriate databases
    for record in iter:
        print("Sending partition...")

        # Update DynamoDB with new record
        update_dynamodb(dynamo_table, record)

        response = dynamo_table.get_item(Key={'id': record['from_id']}) # check to_user data
        json_response = {"firstname": response['Item']['firstname'],
                         "lastname": response['Item']['lastname'],
                         "username": response['Item']['username'],
                         "num_transactions": response['Item']['num_transactions']}
        print("Successfully put " + str(json_response) + " into DynamoDB")

        response = dynamo_table.get_item(Key={'id': record['to_id']}) # check from_user data
        json_response = {"firstname": response['Item']['firstname'],
                         "lastname": response['Item']['lastname'],
                         "username": response['Item']['username'],
                         "num_transactions": response['Item']['num_transactions']}
        print("Successfully put " + str(json_response) + " into DynamoDB")

        # Run approximate transitivity algorithm
        node1 = int(response['Item']['id'])
        node2 = int(response['Item']['id'])
        new_edge = tuple((node1, node2))
        streaming_triangles(redis_db, new_edge)

#        redis_db.set(response['Item']['username'], response['Item']['message'])

#        username = response['Item']['username']
#        print("Successfully put " + read_redis(redis_db, username) + " into Redis")
    # return to the pool for future reuse
    # ConnectionPool.returnConnection(connection)


# To Run:
# sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 kafka-spark-test.py
if __name__ == "__main__":

    # To run on cluster:
    # conf = SparkConf().setAppName("Venmo-Graph-Analytics-Dev").setMaster("spark://ip-172-31-0-135:7077")
    # sc = SparkContext(conf=conf)

    # To run locally:
    sc = SparkContext(appName="Venmo-Graph-Analytics-Dev")

    # Set up resources
    ssc = StreamingContext(sc, 1)   # Set Spark Streaming context


    # brokers = "ec2-50-112-19-115.us-west-2.compute.amazonaws.com:9092,ec2-52-33-162-7.us-west-2.compute.amazonaws.com:9092,ec2-52-89-43-209.us-west-2.compute.amazonaws.com:9092"
    brokers = "ec2-52-25-139-222.us-west-2.compute.amazonaws.com:9092"
    topic = 'Venmo-Transactions-Dev'

    kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    transaction = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    # transaction.pprint()

    ssc.start()
    ssc.awaitTermination()
