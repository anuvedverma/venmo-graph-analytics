import boto3

# Get the service resource.
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Set DynamoDB table
table = dynamodb.Table('venmo-graph-analytics-test')

# # Create an item
# table.put_item(
#    Item={
#         'username': 'janedoe',
#         'message': 'this is a test message',
#         'name': 'Jane Doe'
#     }
# )


# Query an item
response = table.get_item(
    Key={
        'username': 'jgavris',
    }
)

print response
item = response['Item']
# print(item)

# Print out some data about the table.
print(table.item_count)