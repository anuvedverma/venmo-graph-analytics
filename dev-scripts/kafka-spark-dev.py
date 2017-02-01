from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import boto3
import redis


# Extract relevant data from json body
def extract_data(json_body):

    # Sender data
    from_id = json_body['actor']['id']
    from_name = json_body['actor']['name']
    from_username = json_body['actor']['username']
    from_picture = json_body['actor']['picture']

    # Receiver data
    to_id = json_body['transactions'][0]['target']['id']
    to_name = json_body['transactions'][0]['target']['name']
    to_username = json_body['transactions'][0]['target']['username']
    to_picture = json_body['transactions'][0]['target']['picture']

    # Transaction data
    message = json_body['message']
    timestamp = json_body['created_time']

    # Output data dictionary
    data = {'from_id' : from_id,
            'from_name' : from_name,
            'from_username' : from_username,
            'from_picture' : from_picture,
            'to_id' : to_id,
            'to_name' : to_name,
            'to_username' : to_username,
            'to_picture' : to_picture,
            'message' : message,
            'timestamp': timestamp}
    return data

# Send data to DynamoDB
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
    redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com'
    # redis_server = 'localhost'
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    # Route stream data to appropriate databases
    for record in iter:
        print("Sending partition...")
        dynamo_table.update_item(
            Key={
                'id': record['from_id']
            },
            UpdateExpression=''
        )
        # Getting an item
        response = dynamo_table.get_item(Key={'username': record['username']})
        json_response = {"name": response['Item']['name'], "username": response['Item']['username'],
                         "message": response['Item']['message']}
        print("Successfully put " + str(json_response) + " into DynamoDB")


#        redis_db.set(response['Item']['username'], response['Item']['message'])

#        username = response['Item']['username']
#        print("Successfully put " + read_redis(redis_db, username) + " into Redis")
    # return to the pool for future reuse
    # ConnectionPool.returnConnection(connection)

def read_redis(redis_db, key):
    return str(redis_db.get(key))

# Update DynamoDB according to new transaction data
def update_dynamodb(table, data_dict):
    # Creating an item
    table.put_item(
       Item={
            'username': data_dict['username'],
            'name': data_dict['name'],
            'message': data_dict['message'],
        }
    )
    return data_dict


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