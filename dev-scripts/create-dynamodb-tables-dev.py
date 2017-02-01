import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# table = dynamodb.Table('venmo-graph-analytics-dev')
# table.delete()

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='venmo-graph-analytics-dev',
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'id',
            'AttributeType': 'N'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='venmo-graph-analytics-dev')