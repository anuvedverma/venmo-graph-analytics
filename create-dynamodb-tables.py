import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Create the DynamoDB table.
table = dynamodb.create_table(
    TableName='venmo-graph-analytics-test',
    KeySchema=[
        {
            'AttributeName': 'username',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'username',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)

# Wait until the table exists.
table.meta.client.get_waiter('table_exists').wait(TableName='users')


# table = dynamodb.Table('users')
#
# # Creating an item
# table.put_item(
#    Item={
#         'username': 'janedoe',
#         'first_name': 'Jane',
#         'last_name': 'Doe',
#         'age': 25,
#         'account_type': 'standard_user',
#     }
# )
#
# # Getting an item
# response = table.get_item(
#     Key={
#         'username': 'janedoe',
#         'last_name': 'Doe'
#     }
# )
#
# item = response['Item']['first_name']
# print(item)
#
#
# Print out some data about the table.
print(table.item_count)