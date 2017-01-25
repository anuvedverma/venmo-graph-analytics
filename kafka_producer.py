import botocore
import boto3
import threading, logging, time

from kafka import KafkaConsumer, KafkaProducer


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='52.25.139.222:9092')
        # producer = KafkaProducer(bootstrap_servers='localhost:9092')

        bucket_name = 'venmo-json'
        bucket = self.__get_s3_bucket__(bucket_name)
        for obj in bucket.objects.limit(10):
            obj_body = obj.get()['Body']
            json_body = obj_body.read()
            producer.send('Venmo-Transactions-Test', json_body)
            time.sleep(0.5)
            print json_body

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



    # def __incTypeCount__(self, table, crime_type):
    #     print("BEFORE: " + crime_type + ": " + str(table.row(crime_type)))
    #     count_dict = table.row(crime_type)
    #     count = 0
    #     if count_dict:
    #         count = int(count_dict['crime_type:count'])
    #     updated_dict = {'crime_type:count': str(count + 1)}
    #     table.put(crime_type, updated_dict)
    #     print("AFTER: " + crime_type + ": " + str(table.row(crime_type)))



if __name__ == "__main__":
    print "Entered main"
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
        )

    producer = Producer()
    producer.start()
    time.sleep(15)