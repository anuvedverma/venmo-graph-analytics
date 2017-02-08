import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import decimal
import rethinkdb as r


# Get the service resource.
# dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:8000')
conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev').repl()
users_table = r.table('users')


# Set DynamoDB table
# table = dynamodb.Table('venmo-graph-analytics-dev')


# Create an item
def create_item(table, data):
    print("creating item")
    item = {'id': data['id'],
            'username': data['username'],
            'firstname': data['firstname'],
            'lastname': data['lastname'],
            'picture': data['picture'],
            'red_neighbors': [data['red_neighbor']],
            'blue_neighbors': [data['blue_neighbor']],
            'yellow_neighbors': [data['yellow_neighbor']],
            'green_neighbors': [data['green_neighbor']],
            'black_neighbors': [data['black_neighbor']],
            'num_transactions': 1
            }
    table.insert(item).run()


# Update an existing item
def update_item(table, data):
    print("updating item")
    table.get(data['id']).update({'username': data['username'],
                                  'firstname': data['firstname'],
                                  'lastname': data['lastname'],
                                  'picture': data['picture']}).run()
    table.get(data['id']).update({'num_transactions': r.row['num_transactions'].add(1)}).run()
    table.get(data['id']).update({'red_neighbors': r.row['red_neighbors'].append(data['red_neighbor'])}).run()
    table.get(data['id']).update({'blue_neighbors': r.row['blue_neighbors'].append(data['blue_neighbor'])}).run()
    table.get(data['id']).update({'yellow_neighbors': r.row['yellow_neighbors'].append(data['yellow_neighbor'])}).run()
    table.get(data['id']).update({'green_neighbors': r.row['green_neighbors'].append(data['green_neighbor'])}).run()
    table.get(data['id']).update({'black_neighbors': r.row['black_neighbors'].append(data['black_neighbor'])}).run()

#     table.update_item(
#         Key={
#             'id': data['id']
#         },
#         UpdateExpression='SET num_transactions = num_transactions + :inc,' +
#                             # data['color'] + ' = ' + data['color'] + ' + :inc,' +
#                          '''username = :username,
#                             firstname = :firstname,
#                             lastname = :lastname,
#                             picture = :picture,
#                             red_neighbors = list_append(red_neighbors, :red_neighbor),
#                             blue_neighbors = list_append(blue_neighbors, :blue_neighbor),
#                             yellow_neighbors = list_append(yellow_neighbors, :yellow_neighbor),
#                             green_neighbors = list_append(green_neighbors, :green_neighbor),
#                             black_neighbors = list_append(black_neighbors, :black_neighbor)''',
#
#         ExpressionAttributeValues={
#             ':username': data['username'],
#             ':firstname': data['firstname'],
#             ':lastname': data['lastname'],
#             ':picture': data['picture'],
#             ':red_neighbor': [data['red_neighbor']],
#             ':blue_neighbor': [data['blue_neighbor']],
#             ':yellow_neighbor': [data['yellow_neighbor']],
#             ':green_neighbor': [data['green_neighbor']],
#             ':black_neighbor': [data['black_neighbor']],
#             ':inc': 1
#         }
#     )


data = {'id': 46946,
        'username': 'Nicole-Rivera',
        'firstname': 'Nicole',
        'lastname': 'Rivera',
        'picture': 'https://venmopics.appspot.com/u/v2/n/ab815f20-43b5-43bb-a21f-816ed79249de',
        'red_neighbor': 13,
        'blue_neighbor': None,
        'yellow_neighbor': 46982,
        'green_neighbor': 25,
        'black_neighbor': None
        }

# if users_table.get(data['id']).run() is None:
#     create_item(users_table, data)
# else:
#     update_item(users_table, data)

# try:
#     update_item(data)
# except ClientError as e:
#     print(e.response['Error']['Message'])
#     # print(e.response)
#     create_item(data)

# users_table.insert(data, conflict='update').run()
# users_table.filter(r.row['id'] == 46946).update({'views': (r.row['views']+1).default(0)})
# users_table.get(46946)['red_neighbor'].append(46981).run()
# r.db_drop('venmo_graph_analytics_dev').run(conn)
# r.db_create('venmo_graph_analytics_dev').run(conn)

# rethinkdb_table = rethinkdb.table_create('users').run()
response = users_table.get(46946).run()
json_response = {'id': response['id'],
                 'username': response['username'],
                 }
print(json_response)
# cursor = users_table.run()
# for document in cursor:
#     print(document['id'])
# print r.db_list().run()


# Query an item
# response = table.get_item(
#     Key={
#         'id': data['id'],
#     }
# )

# print response
# item = response['Item']
# print(item)

# Print out some data about the table.
# print(table.item_count)
