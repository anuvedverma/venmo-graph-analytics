from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
# import boto3
import redis
import random
import json
import rethinkdb as r


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
    print("creating item")
    item = {'id': data['id'],
            'username': data['username'],
            'firstname': data['firstname'],
            'lastname': data['lastname'],
            'picture': data['picture'],
            'red_neighbors': [data['red_neighbor']],
            'blue_neighbors': [data['blue_neighbor']],
            'yellow_neighbors': [data['yellow_neighbor']],
            'green_neighbors': [data['green_neighbor']],
            'black_neighbors': [data['black_neighbor']],
            'num_transactions': 1
            }
    table.insert(item).run()


# Update an existing item
def update_item(table, data):
    print("updating item")
    table.get(data['id']).update({'username': data['username'],
                                  'firstname': data['firstname'],
                                  'lastname': data['lastname'],
                                  'picture': data['picture']}).run()
    table.get(data['id']).update({'num_transactions': r.row['num_transactions'].add(1)}).run()
    table.get(data['id']).update({'red_neighbors': r.row['red_neighbors'].append(data['red_neighbor'])}).run()
    table.get(data['id']).update({'blue_neighbors': r.row['blue_neighbors'].append(data['blue_neighbor'])}).run()
    table.get(data['id']).update({'yellow_neighbors': r.row['yellow_neighbors'].append(data['yellow_neighbor'])}).run()
    table.get(data['id']).update({'green_neighbors': r.row['green_neighbors'].append(data['green_neighbor'])}).run()
    table.get(data['id']).update({'black_neighbors': r.row['black_neighbors'].append(data['black_neighbor'])}).run()


# Update DynamoDB according to new transaction data
def update_rethinkdb(table, data_dict):
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

    if table.get(sender_data['id']).run() is None:
        create_item(table, sender_data)
    else:
        update_item(table, sender_data)

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

    if table.get(receiver_data['id']).run() is None:
        create_item(table, receiver_data)
    else:
        update_item(table, receiver_data)


# Send data to RethinkDB/Redis databases
def send_partition(rdd):

    # DynomoDB connection
    # dynamodb = boto3.resource('dynamodb',
    #                         aws_access_key_id='dummy-access-id',
    #                         aws_secret_access_key='dummy-secret-access-key',
    #                         region_name='us-west-2',
    #                         endpoint_url='http://localhost:8000')  # Set DynamoDB connection (local)
    # dynamodb = boto3.resource('dynamodb',
    #                         aws_access_key_id='dummy-access-id',
    #                         aws_secret_access_key='dummy-secret-access-key',
    #                         region_name='us-west-2',
    #                         endpoint_url='http://ec2-52-33-8-227.us-west-2.compute.amazonaws.com:8000') # Set DynamoDB connection (cluster)
    # dynamo_table = dynamodb.Table('venmo-graph-analytics-dev')  # Set DynamoDB table

    # RethinkDB connection
    conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev').repl()
    users_table = r.table('users')

    # Redis connection
    redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com' # Set Redis connection (local)
    # redis_server = 'localhost' # Set Redis connection (cluster)
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    for record in rdd:
        print("Sending partition...")

        # Update DynamoDB with new record
        # update_dynamodb(dynamo_table, record)
        update_rethinkdb(users_table, record)

        # response = dynamo_table.get_item(Key={'id': record['from_id']}) # check to_user data
        response = users_table.get(record['from_id']).run() # check to_user data
        json_response = {"id": response['id'],
                         "firstname": response['firstname'],
                         "lastname": response['lastname'],
                         "username": response['username'],
                         "num_transactions": response['num_transactions']}
        redis_db.set(response['username'], response['id'])
        print("Successfully put " + str(json_response) + " into RethinkDB and Redis")

        response = users_table.get(record['to_id']).run() # check from_user data
        json_response = {"id": response['id'],
                         "firstname": response['firstname'],
                         "lastname": response['lastname'],
                         "username": response['username'],
                         "num_transactions": response['num_transactions']}
        redis_db.set(response['username'], response['id'])
        print("Successfully put " + str(json_response) + " into RethinkDB and Redis")


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

    ssc.start()
    ssc.awaitTermination()
