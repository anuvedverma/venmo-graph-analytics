#!/usr/bin/python

# Used https://github.com/dpkp/kafka-python/blob/master/example.py as starting point

import botocore
import boto3
import threading, logging, time
from kafka import KafkaProducer


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        bucket_name = 'venmo-json'
        bucket = self.__get_s3_bucket__(bucket_name)

        # Send data from S3 to Kafka queue
        for obj in bucket.objects.filter(Prefix='2017_*'):
            data = obj.get()['Body']
            json_body = data.read().splitlines()
            for json_obj in json_body:
                producer.send('venmo-transactions', json_obj)
                time.sleep(0.001)
                print json_obj + '\n' + '=================================================================' + '\n'

    # Access S3 bucket
    def __get_s3_bucket__(self, bucket_name):
        s3 = boto3.resource('s3')
        try:
            s3.meta.client.head_bucket(Bucket=bucket_name)
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                exists = False
                print (e.response['404 Error: bucket not found'])
            else:
                print (e.response['Error'])
            return None
        else:
            return s3.Bucket(bucket_name)


if __name__ == "__main__":

    producer = Producer()
    producer.start()
    while True:
        time.sleep(10)