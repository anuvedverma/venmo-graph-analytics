#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

# import happybase
import json
import threading, logging, time
from kafka import KafkaConsumer


class Consumer(threading.Thread):
    daemon = True

    # Constructor sets up hbase connection
    # def __init__(self):
    #     super(Consumer, self).__init__()
    #    self.hbase = happybase.Connection('hdp-m')
    #    self.hbase.open()

    # Thread sets up consumer and consumes kafka messages
    def run(self):
        consumer = KafkaConsumer(bootstrap_servers='52.25.139.222:9092',
                auto_offset_reset='latest')
        consumer.subscribe(['Venmo-Transactions-Test'])

        for message in consumer:
            msg = str(message.value)
            # crime_type = msg.split('|')[0]
            # crime_street = msg.split('|')[1]
            # crime_loctype = msg.split('|')[2]

            print (msg)
            # self.__incTypeCount__(self.hbase.table('anuvedverma_crimes_by_type'), crime_type)
            # self.__incStreetCount__(self.hbase.table('anuvedverma_crimes_by_street'), crime_street)
            # self.__incLocationTypeCount__(self.hbase.table('anuvedverma_crimes_by_location_type'), crime_loctype)
            # self.__incTypeAndLocationTypeCount__(self.hbase.table('anuvedverma_crimes_by_type_and_loctype'), crime_type, crime_loctype)

    # def __incTypeCount__(self, table, crime_type):
    #     print("BEFORE: " + crime_type + ": " + str(table.row(crime_type)))
    #     count_dict = table.row(crime_type)
    #     count = 0
    #     if count_dict:
    #         count = int(count_dict['crime_type:count'])
    #     updated_dict = {'crime_type:count' : str(count+1)}
    #     table.put(crime_type, updated_dict)
    #     print("AFTER: " + crime_type + ": " + str(table.row(crime_type)))
    #
    # def __incStreetCount__(self, table, crime_street):
    #     print("BEFORE: " + crime_street + ": " + str(table.row(crime_street)))
    #     count_dict = table.row(crime_street)
    #     count = 0
    #     if count_dict:
    #         count = int(count_dict['crime_street:count'])
    #     updated_dict = {'crime_street:count' : str(count+1)}
    #     table.put(crime_street, updated_dict)
    #     print("AFTER: " + crime_street + ": " + str(table.row(crime_street)))
    #
    # def __incLocationTypeCount__(self, table, crime_location_type):
    #     print("BEFORE: " + crime_location_type + ": " + str(table.row(crime_location_type)))
    #     count_dict = table.row(crime_location_type)
    #     count = 0
    #     if count_dict:
    #         count = int(count_dict['crime_location_type:count'])
    #     updated_dict = {'crime_location_type:count' : str(count+1)}
    #     table.put(crime_location_type, updated_dict)
    #     print("AFTER: " + crime_location_type + ": " + str(table.row(crime_location_type)))
    #
    # def __incTypeAndLocationTypeCount__(self, table, crime_type, crime_loctype):
    #     crime_type_loctype = crime_type + '|' + crime_loctype
    #     print("BEFORE: " + crime_type_loctype + ": " + str(table.row(crime_type_loctype)))
    #     count_dict = table.row(crime_type_loctype)
    #     count = 0
    #     if count_dict:
    #         count = int(count_dict['crime_type_loctype:count'])
    #     updated_dict = {'crime_type_loctype:count' : str(count+1)}
    #     table.put(crime_type_loctype, updated_dict)
    #     print("AFTER: " + crime_type_loctype + ": " + str(table.row(crime_type_loctype)))


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
            print("Listening for new messages in topic: 'Venmo-Transactions-Test'...")
            time.sleep(15)
