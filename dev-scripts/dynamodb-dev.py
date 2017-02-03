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
           'red_neighbors': [data['red_neighbor']],
           'blue_neighbors': [data['blue_neighbor']],
           'yellow_neighbors': [data['yellow_neighbor']],
           'green_neighbors': [data['green_neighbor']],
           'black_neighbors': [data['black_neighbor']],
           # 'red': int(data['color'] == 'red'),
           # 'blue': int(data['color'] == 'blue'),
           # 'yellow': int(data['color'] == 'yellow'),
           # 'green': int(data['color'] == 'green'),
           # 'black': int(data['color'] == 'black'),
           'num_transactions': 1
       }
    )

# Update an existing item
def update_item(data):
    table.update_item(
        Key={
            'id': data['id']
        },
        UpdateExpression='SET num_transactions = num_transactions + :inc,' +
                            # data['color'] + ' = ' + data['color'] + ' + :inc,' +
                         '''username = :username,
                            firstname = :firstname,
                            lastname = :lastname,
                            picture = :picture,
                            red_neighbors = list_append(red_neighbors, :red_neighbor),
                            blue_neighbors = list_append(blue_neighbors, :blue_neighbor),
                            yellow_neighbors = list_append(yellow_neighbors, :yellow_neighbor),
                            green_neighbors = list_append(green_neighbors, :green_neighbor),
                            black_neighbors = list_append(black_neighbors, :black_neighbor)''',

        ExpressionAttributeValues={
            ':username': data['username'],
            ':firstname': data['firstname'],
            ':lastname': data['lastname'],
            ':picture': data['picture'],
            ':red_neighbor': [data['red_neighbor']],
            ':blue_neighbor': [data['blue_neighbor']],
            ':yellow_neighbor': [data['yellow_neighbor']],
            ':green_neighbor': [data['green_neighbor']],
            ':black_neighbor': [data['black_neighbor']],
            ':inc': 1
        }
    )



data = {'id' : 46946,
        'username': 'Nicole-Rivera',
        'firstname': 'Nicole',
        'lastname': 'Rivera',
        'picture': 'https://venmopics.appspot.com/u/v2/n/ab815f20-43b5-43bb-a21f-816ed79249de',
        'red_neighbor': None,
        'blue_neighbor': None,
        'yellow_neighbor': 46982,
        'green_neighbor': 46981,
        'black_neighbor': None
        }
try:
    update_item(data)
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