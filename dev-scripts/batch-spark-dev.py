from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from botocore.exceptions import ClientError
from kafka import KafkaProducer
from sets import Set
# import boto3
import redis
import random
import json
from pymoji import PyMoji
import rethinkdb as r

RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'
BLACK = 'black'

# Extract relevant data from json body
def extract_data(json_body):

    json_body = json.loads(json_body)

    # Sender data
    from_id = json_body['actor']['id']
    from_firstname = json_body['actor']['firstname']
    from_lastname = json_body['actor']['lastname']
    from_username = json_body['actor']['username']
    from_picture = json_body['actor']['picture']

    # Receiver data
    if 'id' in json_body['transactions'][0]['target']:
        to_id = json_body['transactions'][0]['target']['id']
    else:
        return None
    if 'firstname' in json_body['transactions'][0]['target']:
        to_firstname = json_body['transactions'][0]['target']['firstname']
    else:
        to_firstname = "N/A"
    if 'lastname' in json_body['transactions'][0]['target']:
        to_lastname = json_body['transactions'][0]['target']['lastname']
    else:
        to_lastname = "N/A"
    if 'username' in json_body['transactions'][0]['target']:
        to_username = json_body['transactions'][0]['target']['username']
    else:
        return None
    if 'picture' in json_body['transactions'][0]['target']:
        to_picture = json_body['transactions'][0]['target']['picture']
    else:
        to_picture = "N/A"

    # Transaction data
    message = json_body['message']
    timestamp = json_body['created_time']

    # Filter out invalid values
    if not from_picture:
        from_username = 'N/A'
    if not to_picture:
        to_picture = 'N/A'

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


def color_messages(transaction_data):
    user1 = transaction_data['from_id']
    user2 = transaction_data['to_id']
    message = transaction_data['message']
    colors = analyze_message(message)
    # print("MESSAGE: " + message)
    # print("COLOR: " + str(colors))

    results = []
    for color in colors:
        results.append(tuple((color, user1, user2)))

    return results


def analyze_message(message):
    moji = PyMoji()
    message = moji.encode(message)
    if isinstance(message, str):
        message = unicode(message, "utf-8")
    message = message.encode('utf-8').lower()
    # print(message)

    # Define categorization rules
    foods = ["[:pizza]", "[:hamburger]", "pizza", "food", "burrito",
             "[:fries]", "[:ramen]", "tacos", "dinner", "lunch",
             "[:spaghetti]", "[:poultry_leg]", "breakfast",
             "[:sushi]"]
    drinks = ["[:wine_glass]", "[:cocktail]", "[:tropical_drink]",
              "[:beer]", "[:beers]", "[:tada]", "drinks"]
    transportation = ["[:taxi]", "[:oncoming_taxi]", "[:car]",
                      "[:oncoming_automobile]", "taxi", "uber", "lyft"]
    bills = ["[:bulb]", "[:moneybag]", "[:potable_water]",
             "[:house_with_garden]", "[:house]", " bill",
             "rent", "internet", "utilities", "pg&e", "dues", "cable"]

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


def filter_nones(transaction_data):
    if transaction_data is not None:
        return True
    return False


def filter_blacks(transaction_data):
    if transaction_data[0] == BLACK:
        return False
    return True


def send_to_rethink(rdd):

    # RethinkDB connection
    # conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev').repl()
    # users_table = r.table('users')

    # Update RethinkDB with new record
    # update_rethinkdb(users_table, record)

    pass


# Send data to RethinkDB/Redis databases
def send_to_redis(rdd):

    # Redis connection
    redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com' # Set Redis connection
    # redis_server = 'localhost' # Set Redis connection (cluster)
    redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

    # Kafka connection
    # producer = KafkaProducer(bootstrap_servers='52.35.109.64:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    # producer.send('venmo-transactions', rdd)

    for record in rdd:
        color = record[0]
        edge_list = record[1]

        # print("Record: " + str(record))
        print("Sending partition...")
        # producer.send('venmo-transactions', record)
        redis_db.lpush(color, *edge_list)

        print("Successfully put " + str(redis_db.lrange(color, 0, len(edge_list)-1)) + " into Redis")


# To Run:
# sudo $SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 kafka-spark-test.py
if __name__ == "__main__":

    # Set Spark context
    sc = SparkContext(appName="Venmo-Graph-Analytics-Dev")

    # Read data from S3
    # read_rdd = sc.textFile("s3n://venmo-json/2017_01/*")
    # read_rdd = sc.textFile("s3n://venmo-json/2011_01/*")
    read_rdd = sc.textFile("s3n://venmo-json/2013_01/*")

    # Clean and filter data
    cleaned_rdd = read_rdd.map(lambda x: extract_data(x)).filter(lambda x: filter_nones(x)) # clean json data
    colored_rdd = cleaned_rdd.flatMap(lambda x: color_messages(x)) # classify messages with color
    filtered_rdd = colored_rdd.filter(lambda x: filter_blacks(x)) # filter out black edges
    color_grouped_rdd = filtered_rdd.map(lambda x: (x[0], [tuple((x[1], x[2]))]))\
        .reduceByKey(lambda x, y: x + y) # group to (key: color, value: [edge-list])

    # Send data to DBs
    color_grouped_rdd.foreachPartition(lambda x: send_to_redis(x))
    # color_grouped_rdd.foreachPartition(lambda x: send_to_rethink(x))

    # output = colored_rdd.take(500)
    # colored_rdd_count = colored_rdd.count()
    # filtered_rdd_count = filtered_rdd.count()
    # color_grouped_rdd_count = color_grouped_rdd.count()