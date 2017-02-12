#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

import threading, logging, time
import random
import pickle
import json
import numpy as np
import redis
from kafka import KafkaConsumer
from pymoji import PyMoji
from pybloom import ScalableBloomFilter
from sets import Set

RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'


class StreamingTriangles(threading.Thread):
    daemon = True

    # Constructor sets up Redis connection and algorithm vars
    def __init__(self):
        super(StreamingTriangles, self).__init__()
        self.redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com'
        # redis_server = 'localhost'
        self.redis_db = redis.StrictRedis(host=self.redis_server, port=6379, db=0)

        self.edge_res_size = 20000
        self.wedge_res_size = 20000
        self.bloom_filter = ScalableBloomFilter(mode=ScalableBloomFilter.SMALL_SET_GROWTH)

        self.edge_count = {RED: 0,
                           BLUE: 0,
                           YELLOW: 0,
                           GREEN: 0}

        self.total_wedges = {RED: 0,
                           BLUE: 0,
                           YELLOW: 0,
                           GREEN: 0}

        self.edge_res = {RED: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         BLUE: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         YELLOW: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         GREEN: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)]}

        self.wedge_res = {RED: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          BLUE: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          YELLOW: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          GREEN: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)]}

        self.is_closed = {RED: [False for _ in xrange(self.wedge_res_size)],
                          BLUE: [False for _ in xrange(self.wedge_res_size)],
                          YELLOW: [False for _ in xrange(self.wedge_res_size)],
                          GREEN: [False for _ in xrange(self.wedge_res_size)]}

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        # consumer = KafkaConsumer(bootstrap_servers='52.25.139.222:9092',
        #         auto_offset_reset='largest')
        consumer = KafkaConsumer(bootstrap_servers='52.35.109.64:9092')
        consumer.subscribe(['venmo-transactions'])

        for message in consumer:
            msg = str(message.value)
            new_edge = self.__extract_edge__(msg)
            colors = self.__analyze_message__(msg)
            for color in colors:
                colored_edge = tuple((color, new_edge))
                if colored_edge not in self.bloom_filter and -1 not in new_edge:
                    self.__streaming_triangles__(self.redis_db, new_edge, color)
                    self.bloom_filter.add(colored_edge)

    def __analyze_message__(self, json_obj):
        json_data = json.loads(json_obj)
        message = json_data['message'] # message data

        moji = PyMoji()
        message = moji.encode(message)
        if isinstance(message, str):
            message = unicode(message, "utf-8")
        message = message.encode('utf-8').lower()
        print(message)

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

        return colors


    def __streaming_triangles__(self, redis_db, new_edge, color):
        k = self.__update__(new_edge, color)
        transitivity = 3 * k
        redis_db.set(str(color + '_transitivity'), transitivity)

    def __update__(self, new_edge, color):

        self.edge_count[color] += 1
        updated_edge_res = False

        for i in range(len(self.wedge_res[color])):
            if self.__is_closed_by__(self.wedge_res[color][i], new_edge):
                self.is_closed[color][i] = True

        for i in range(len(self.edge_res[color])):
            x = random.uniform(0, 1)
            if x < (1 / float(self.edge_count[color])):
                self.edge_res[color][i] = new_edge
                updated_edge_res = True

        if updated_edge_res:
            new_wedges = []
            for i in range(len(self.edge_res[color])):
                if self.__creates_wedge__(self.edge_res[color][i], new_edge):
                    new_wedges.append(self.__get_wedge__(self.edge_res[color][i], new_edge))
            self.total_wedges[color] += len(new_wedges)
            for i in range(len(self.wedge_res[color])):
                x = random.uniform(0, 1)
                if self.total_wedges[color] > 0 and x < (len(new_wedges) / float(self.total_wedges[color])):
                    w = random.choice(new_wedges)
                    self.wedge_res[color][i] = w
                    self.is_closed[color][i] = False

        return np.sum(self.is_closed[color])/float(len(self.is_closed[color]))

    # Extract relevant data from json body
    def __extract_edge__(self, json_obj):
        json_data = json.loads(json_obj)
        try:
            from_id = int(json_data['actor']['id']) # Sender data
            to_id = int(json_data['transactions'][0]['target']['id']) # Receiver data
        except:
            from_id = -1
            to_id = -1
        edge = sorted(tuple((from_id, to_id)))
        return edge

    # Extract wedge from adjacent edges
    def __get_wedge__(self, edge1, edge2):
        if edge1[0] == edge2[0]:
            return tuple((edge2[1], edge1[0], edge1[1]))
        if edge1[0] == edge2[1]:
            return tuple((edge2[0], edge1[0], edge1[1]))
        if edge1[1] == edge2[0]:
            return tuple((edge2[1], edge1[1], edge1[0]))
        if edge1[1] == edge2[1]:
            return tuple((edge2[0], edge1[1], edge1[0]))
        return None

    # Check if input edge closes input wedge
    def __is_closed_by__(self, wedge, edge):
        if (wedge[0] == edge[0] and wedge[2] == edge[1]) or (wedge[0] == edge[1] and wedge[2] == edge[0]):
            return True
        return False

    # Check if input edges create a wedge
    def __creates_wedge__(self, edge1, edge2):
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
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)

    thread = StreamingTriangles()

    while True:
        if not thread.isAlive():
            print("Starting Kafka consumer...")
            thread.start()
            print("Started Kafka consumer.")
        else:
            print("Listening for new messages in topic: 'venmo-transactions'...")
            time.sleep(15)
