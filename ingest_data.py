import botocore
import boto3


def get_s3_bucket(bucket_name):
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


def get_first_object(bucket):
    for obj in bucket.objects.limit(1):
        first_obj = obj
    return first_obj.get()['Body']


def get_object_line(object_body):
    test = []
    char = None
    while char != '\n':
        char = object_body.read(1)
        test.append(char)
    return ''.join(test)


bucket_name = 'venmo-json'
bucket = get_s3_bucket(bucket_name)
first_object = get_first_object(bucket)
print get_object_line(first_object)
