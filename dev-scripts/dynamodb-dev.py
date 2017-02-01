import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import decimal

# Get the service resource.
dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')

# Set DynamoDB table
table = dynamodb.Table('venmo-graph-analytics-dev')

# Create an item
def create_item(data):
    table.put_item(
       Item={
           'id': data['id'],
           'username': data['username'],
           'firstname': data['firstname'],
           'lastname': data['lastname'],
           'picture': data['picture'],
           'neighbors': set([data['neighbor']]),
           'num_transactions': 1,
           'red': int(data['color'] == 'red'),
           'blue': int(data['color'] == 'blue'),
           'yellow': int(data['color'] == 'yellow'),
           'green': int(data['color'] == 'green'),
           'black': int(data['color'] == 'black'),
        }
    )


data = {'id' : 46946,
        'username': 'Nicole-Rivera',
        'firstname': 'Nicole',
        'lastname': 'Rivera',
        'picture': 'https://venmopics.appspot.com/u/v2/n/ab815f20-43b5-43bb-a21f-816ed79249de',
        'color': 'blue',
        'neighbor': 46981
        }
try:
    table.update_item(
        Key={
            'id': data['id']
        },
        UpdateExpression='SET num_transactions = num_transactions + :inc,' +
                            data['color'] + ' = ' + data['color'] + ' + :inc,' +
                         '''username = :username,
                            firstname = :firstname,
                            lastname = :lastname,
                            picture = :picture,
                            neighbors = list_append(neighbors, :neighbor)''',

        ExpressionAttributeValues={
            ':username': data['username'],
            ':firstname': data['firstname'],
            ':lastname': data['lastname'],
            ':picture': data['picture'],
            ':inc': 1,
            ':neighbor': [data['neighbor']]
        }
    )
except ClientError as e:
    print(e.response['Error']['Message'])
    # print(e.response)
    create_item(data)



# Query an item
response = table.get_item(
    Key={
        'id': data['id'],
    }
)

# print response
item = response['Item']
print(item)


# table.update_item(
#     Key={
#         'id': '34423'
#     },
#     UpdateExpression=''
# )


# Print out some data about the table.
print(table.item_count)