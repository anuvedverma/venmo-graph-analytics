from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
from pymoji import PyMoji
from sets import Set
import boto3
import redis
import pickle
import numpy as np
import random
import json



RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'
BLACK = 'black'


# Extract relevant data from json body
def extract_data(json_body):

    json_body = json.loads(json_body)

    try:
        # Sender data
        from_id = json_body['actor']['id']
        from_firstname = json_body['actor']['firstname']
        from_lastname = json_body['actor']['lastname']
        from_username = json_body['actor']['username']
        from_picture = json_body['actor']['picture']
        is_business = json_body['actor']['is_business']

        # Receiver data
        to_id = json_body['transactions'][0]['target']['id']
        to_firstname = json_body['transactions'][0]['target']['firstname']
        to_lastname = json_body['transactions'][0]['target']['lastname']
        to_username = json_body['transactions'][0]['target']['username']
        to_picture = json_body['transactions'][0]['target']['picture']
        is_business = is_business or json_body['transactions'][0]['target']['is_business']

        if is_business is True:
            return None

        # Transaction data
        message = json_body['message']
        timestamp = json_body['created_time']
    except:
        return None

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


# Generate colored edge
def color_messages(transaction_data):
    user1 = transaction_data['from_id']
    user2 = transaction_data['to_id']
    message = transaction_data['message']
    colors = analyze_message(message)

    results = []
    for color in colors:
        results.append(tuple((color, user1, user2)))

    return results


# Assign colors to message based on emoji/text content
def analyze_message(message):
    moji = PyMoji()
    message = moji.encode(message)
    if isinstance(message, str):
        message = unicode(message, "utf-8")
    message = message.encode('utf-8').lower()

    # Define categorization rules
    foods = ["pizza", "hamburger", "food", "burrito", "chinese", "indian",
             "fries", "ramen", "taco", "dinner", "lunch",
             "spaghetti", "poultry_leg", "breakfast", "sushi"]
    drinks = ["wine", "cocktail", "drink", " bar", "alcohol",
              "beer", "[:tada]", "club", "vegas"]
    transportation = ["taxi", "[:car]", "[:oncoming_automobile]",
                      "uber", "lyft", "ride", "drive", "driving"]
    bills = ["bulb", "[:moneybag]", "water", "[:house_with_garden]",
             "[:house]", " bill", "rent", "internet", "utilities",
             "pg&e", "dues", "cable"]

    colors = Set([])

    # Check for food-related content
    if any(food in message for food in foods):
        colors.add(RED)

    # Check for drink-related content
    if any(drink in message for drink in drinks):
        colors.add(BLUE)

    # Check for transportation-related content
    if any(transport in message for transport in transportation):
        colors.add(YELLOW)

    # Check for transportation-related content
    if any(bill in message for bill in bills):
        colors.add(GREEN)

    # Tag remaining items for removal
    if len(colors) == 0:
        colors.add(BLACK)

    return colors


# Filter out nones
def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False


# Filter out uncategorized/uncolored messages
def filter_blacks(transaction_data):
    if transaction_data[0] == BLACK:
        return False
    return True


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

    # Redis connection
    redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com' # Set Redis connection (local)
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    # Init edge/wedge reservoirs
    edge_res_size = 2000
    wedge_res_size = 2000

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

        # Run approximate transitivity algorithm
        # node1 = int(response['Item']['id'])
        # node2 = int(response['Item']['id'])
        # new_edge = tuple((node1, node2))
        # streaming_triangles(redis_db, new_edge)

# To Run:
# sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 kafka-spark-test.py
if __name__ == "__main__":


    # Set up resources
    sc = SparkContext(appName="Venmo-Graph-Analytics")
    ssc = StreamingContext(sc, 1)  # Set Spark Streaming context
    ssc.checkpoint("checkpoint")

    # brokers = "ec2-50-112-19-115.us-west-2.compute.amazonaws.com:9092,ec2-52-33-162-7.us-west-2.compute.amazonaws.com:9092,ec2-52-89-43-209.us-west-2.compute.amazonaws.com:9092"
    brokers = "ec2-52-25-139-222.us-west-2.compute.amazonaws.com:9092"
    topic = 'Venmo-Transactions-Dev'

    kafka_stream = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    transaction = kafka_stream.map(lambda kafka_response: json.loads(kafka_response[1]))\
        .map(lambda json_body: extract_data(json_body))


    def update_func(new_values, last_sum):
        sum(new_values) + (last_sum or 0)

    running_counts = transaction.map(lambda data: ('count', 1)).updateStateByKey(update_func)
#        .foreachRDD(lambda rdd: rdd.foreachPartition(send_partition))
    # transaction.pprint()

    ssc.start()
    ssc.awaitTermination()