from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import boto3
import re


# Extract relevant data from json body
def extract_data(json_body):
    name = json_body['actor']['name']
    username = json_body['actor']['username']
    message = json_body['message']
    data = {'name' : name, 'username' : username, 'message' : message}
    return data

# Send data to DynamoDB
def send_partition(iter):
    # ConnectionPool is a static, lazily initialized pool of connections
    # connection = ConnectionPool.getConnection()
    dynamodb = boto3.resource('dynamodb',
                            aws_access_key_id='dummy-access-id',
                            aws_secret_access_key='dummy-secret-access-key',
                            region_name='us-west-2',
                            endpoint_url='http://localhost:8000')  # Set DynamoDB connection (local)
    # dynamodb = boto3.resource('dynamodb') # Set DynamoDB connection (cluster)
    table = dynamodb.Table('venmo-graph-analytics-test')  # Set DynamoDB table

    for record in iter:
        print("Sending partition...")
        print(record)
        table.put_item(
            Item={
                'username': record['username'],
                'name': record['name'],
                'message': record['message'],
            }
        )
    # return to the pool for future reuse
    # ConnectionPool.returnConnection(connection)


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
    # conf = SparkConf().setAppName("Venmo-Graph-Analytics-Test").setMaster("spark://ip-172-31-0-135:7077")
    # sc = SparkContext(conf=conf)

    # To run locally:
    sc = SparkContext(appName="Venmo-Graph-Analytics-Test")

    # Set up resources
    ssc = StreamingContext(sc, 1)   # Set Spark Streaming context


    # brokers = "ec2-50-112-19-115.us-west-2.compute.amazonaws.com:9092,ec2-52-33-162-7.us-west-2.compute.amazonaws.com:9092,ec2-52-89-43-209.us-west-2.compute.amazonaws.com:9092"
    brokers = "ec2-52-25-139-222.us-west-2.compute.amazonaws.com:9092"

    kafka_stream = KafkaUtils.createDirectStream(ssc, ['Venmo-Transactions-Test'], {"metadata.broker.list": brokers})

    transaction = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))\
        .foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))

    # transaction.pprint()

    # lines = kafka_stream.map(lambda x: x[1])
    # counts = lines.flatMap(lambda line: line.split(" ")) \
    #     .map(lambda word: (word, 1)) \
    #     .reduceByKey(lambda a, b: a+b)
    # counts.pprint()

    ssc.start()
    ssc.awaitTermination()