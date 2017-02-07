#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

import threading, logging, time
import random
import pickle
import json
import numpy as np
import redis
from kafka import KafkaConsumer

RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'
BLACK = 'black'

class StreamingTriangles(threading.Thread):
    daemon = True

    # Constructor sets up Redis connection and algorithm vars
    def __init__(self):
        super(StreamingTriangles, self).__init__()
        self.redis_server = 'ec2-52-33-8-227.us-west-2.compute.amazonaws.com'
        # redis_server = 'localhost'
        self.redis_db = redis.StrictRedis(host=self.redis_server, port=6379, db=0)

        self.edge_res_size = 20000
        self.wedge_res_size = 20000

        self.edge_count = {RED: 0,
                           BLUE: 0,
                           YELLOW: 0,
                           GREEN: 0,
                           BLACK: 0}

        self.total_wedges = {RED: 0,
                           BLUE: 0,
                           YELLOW: 0,
                           GREEN: 0,
                           BLACK: 0}

        self.edge_res = {RED: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         BLUE: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         YELLOW: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         GREEN: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)],
                         BLACK: [list(tuple((0, 0))) for _ in xrange(self.edge_res_size)]}

        self.wedge_res = {RED: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          BLUE: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          YELLOW: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          GREEN: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)],
                          BLACK: [list(tuple((0, 0, 0))) for _ in xrange(self.wedge_res_size)]}

        self.is_closed = {RED: [False for _ in xrange(self.wedge_res_size)],
                          BLUE: [False for _ in xrange(self.wedge_res_size)],
                          YELLOW: [False for _ in xrange(self.wedge_res_size)],
                          GREEN: [False for _ in xrange(self.wedge_res_size)],
                          BLACK: [False for _ in xrange(self.wedge_res_size)]}

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        # consumer = KafkaConsumer(bootstrap_servers='52.25.139.222:9092',
        #         auto_offset_reset='largest')
        consumer = KafkaConsumer(bootstrap_servers='52.25.139.222:9092')
        consumer.subscribe(['Venmo-Transactions-Dev'])

        for message in consumer:
            msg = str(message.value)
            new_edge = self.__extract_edge__(msg)
            colors = self.__analyze_message__(msg)
            for color in colors:
                if colors[color] == True:
                    self.__streaming_triangles__(self.redis_db, new_edge, color)
            # self.__incTypeCount__(self.hbase.table('anuvedverma_crimes_by_type'), crime_type)
            # print (msg)

    def __analyze_message__(self, json_obj):
        json_data = json.loads(json_obj)
        message = json_data['message'] # message data

        color_map = {}
        # Insert real analysis here
        if random.uniform(0, 1) < 0.75:
            color_map[RED] = True
        else:
            color_map[RED] = False

        if random.uniform(0, 1) < 0.25:
            color_map[BLUE] = True
        else:
            color_map[BLUE] = False

        if random.uniform(0, 1) < 0.5:
            color_map[YELLOW] = True
        else:
            color_map[YELLOW] = False

        if random.uniform(0, 1) < 0.1:
            color_map[GREEN] = True
        else:
            color_map[GREEN] = False

        if random.uniform(0, 1) < 0.33:
            color_map[BLACK] = True
        else:
            color_map[BLACK] = False

        # Update color_map based on analysis results
        output = {RED: color_map[RED],
                  BLUE: color_map[BLUE],
                  YELLOW: color_map[YELLOW],
                  GREEN: color_map[GREEN],
                  BLACK: color_map[BLACK]}
        return output

    def __streaming_triangles__(self, redis_db, new_edge, color):
        k = self.__update__(redis_db, new_edge, color)
        transitivity = 3 * k
        print("Transitivity @ " + color + " edge #" + str(self.edge_count[color]) + ": " + str(transitivity))
        print("Total "  + color + " wedges: " + str(self.total_wedges[color]))
        # if self.edge_count % 1000 == 0:
        redis_db.set(str(color + '_transitivity'), transitivity)
        # print("Edge Res: " + str(self.edge_res))

    def __update__(self, redis_db, new_edge, color):

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

        # print(self.edge_res)
        # print(wedge_res)
        # print(is_closed)

        return np.sum(self.is_closed[color])/float(len(self.is_closed[color]))

    # Extract relevant data from json body
    def __extract_edge__(self, json_obj):
        json_data = json.loads(json_obj)
        from_id = int(json_data['actor']['id']) # Sender data
        to_id = int(json_data['transactions'][0]['target']['id']) # Receiver data
        return tuple((from_id, to_id))

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
            print("Listening for new messages in topic: 'Venmo-Transactions-Dev'...")
            time.sleep(15)
