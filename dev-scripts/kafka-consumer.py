#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

import json
import threading, logging, time
from kafka import KafkaConsumer


class Consumer(threading.Thread):
    daemon = True

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='52.35.109.64:9092',
                auto_offset_reset='latest')
        consumer.subscribe(['venmo-transactions'])

        for message in consumer:
            msg = str(message.value)
            print (msg)

        # blue_consumer = KafkaConsumer(bootstrap_servers='52.35.109.64:9092',
        #         auto_offset_reset='latest')
        # blue_consumer.subscribe(['venmo-blue-transactions'])
        #
        # for message in blue_consumer:
        #     msg = "BLUE: " + str(message.value)
        #     print (msg)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO)

    thread = Consumer()

    while True:
        if not thread.isAlive():
            print("Starting Kafka consumer...")
            thread.start()
            print("Started Kafka consumer.")
        else:
            print("Listening for new messages in topic: 'venmo-transactions'...")
            time.sleep(15)
