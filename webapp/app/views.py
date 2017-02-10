from __future__ import print_function # In python 2.7

from app import app
from flask import jsonify
from flask import render_template
from flask import request
from ast import literal_eval as make_tuple
import sys
import json
import redis
import networkx as nx
from networkx.readwrite import json_graph
import rethinkdb as r
# import boto3


# conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev')
# users_table = r.table('users')

redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com'
    # redis_server = 'localhost'
redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)

@app.route('/')
@app.route('/index')
def index():
    user = {'nickname': 'Miguel'}  # fake user
    mylist = [1, 2, 3, 4]
    # return render_template("index.html", title='Home', list=mylist, )
    return render_template("index.html", title='Home', user=user)

@app.route('/communityinfo')
def community_info():

    # red_transitivity = redis_db.get('red_transitivity')
    # blue_transitivity = redis_db.get('blue_transitivity')
    # yellow_transitivity = redis_db.get('yellow_transitivity')
    # green_transitivity = redis_db.get('green_transitivity')
    # black_transitivity = redis_db.get('black_transitivity')
    # num_triangles = redis_db.get('num_triangles')
    #
    # # print("Query result: " + response)
    # response_dict = {"red_transitivity": red_transitivity,
    #                  "blue_transitivity": blue_transitivity,
    #                  "yellow_transitivity": yellow_transitivity,
    #                  "green_transitivity": green_transitivity,
    #                  "black_transitivity": black_transitivity,
    #                  "num_triangles": num_triangles}

    red_edges = redis_db.lrange('red', 0, -1)
    blue_edges = redis_db.lrange('blue', 0, -1)
    yellow_edges = redis_db.lrange('yellow', 0, -1)
    green_edges = redis_db.lrange('green', 0, -1)

    # process graphs
    red_edges_response = process_graph(red_edges)
    blue_edges_response = process_graph(blue_edges)
    yellow_edges_response = process_graph(yellow_edges)
    green_edges_response = process_graph(green_edges)

    # write json
    json.dump(red_edges_response, open('/Users/anuvedverma/Projects/Insight/venmo-graph-analytics/webapp/app/static/red_graph.json', 'w+'))
    json.dump(blue_edges_response, open('/Users/anuvedverma/Projects/Insight/venmo-graph-analytics/webapp/app/static/blue_graph.json', 'w+'))
    json.dump(yellow_edges_response, open('/Users/anuvedverma/Projects/Insight/venmo-graph-analytics/webapp/app/static/yellow_graph.json', 'w+'))
    json.dump(green_edges_response, open('/Users/anuvedverma/Projects/Insight/venmo-graph-analytics/webapp/app/static/green_graph.json', 'w+'))

    # red_data = json_graph.node_link_data(red_graph)
    response_dict = {"red_edges_response": json.dumps(red_edges_response),
                     "blue_edges_response": json.dumps(blue_edges_response),
                     "yellow_edges_response": json.dumps(yellow_edges_response),
                     "green_edges_response": json.dumps(green_edges_response)}

    return render_template("communityinfo.html", output=response_dict)


def process_graph(edges):
    for i in range(len(edges)):
        edges[i] = make_tuple(edges[i])

    graph = nx.Graph()
    graph.add_edges_from(edges)

    remove = [edge for edge in edges if graph.degree(edge[0]) < 2 and graph.degree(edge[1]) < 2]
    graph.remove_edges_from(remove)

    response = []
    for edge in graph.edges():
        response.append({"source": edge[0], "target": edge[1]})

    print(len(response))
    return response


@app.route('/usersearch')
def user_search():
    return render_template("usersearch.html")

# @app.route("/usersearch", methods=['POST'])
# def username_post():
#     username = request.form["username"]
#     id = redis_db.get(username)
#     print("Received request: " + id)
#
#     response = users_table.get(int(id)).run(conn)
#
#     print(response)
#     num_transactions = response['num_transactions']
#     reds = response['red_neighbors']
#     blues = response['blue_neighbors']
#     yellows = response['yellow_neighbors']
#     greens = response['green_neighbors']
#     blacks = response['black_neighbors']
#
#     user = response['id']
#     red_neighbors = set([x for x in reds if x is not None])
#     blue_neighbors = set([x for x in blues if x is not None])
#     yellow_neighbors = set([x for x in yellows if x is not None])
#     green_neighbors = set([x for x in greens if x is not None])
#     black_neighbors = set([x for x in blacks if x is not None])
#
#     red_edges = bfs(user, red_neighbors, 'red_neighbors')
#     blue_edges = bfs(user, blue_neighbors, 'blue_neighbors')
#     yellow_edges = bfs(user, yellow_neighbors, 'yellow_neighbors')
#     green_edges = bfs(user, green_neighbors, 'green_neighbors')
#     black_edges = bfs(user, black_neighbors, 'black_neighbors')
#
#     num_reds = len([x for x in reds if x is not None])
#     num_blues = len([x for x in blues if x is not None])
#     num_yellows = len([x for x in yellows if x is not None])
#     num_greens = len([x for x in greens if x is not None])
#     num_blacks = len([x for x in blacks if x is not None])
#
#     # print("Query result: " + response)
#     response_dict = {"firstname": response['firstname'],
#                      "lastname": response['lastname'],
#                      "username": response['username'],
#                      "num_transactions": num_transactions,
#                      "ratio_reds": float(num_reds / num_transactions),
#                      "ratio_blues": float(num_blues / num_transactions),
#                      "ratio_yellows": float(num_yellows / num_transactions),
#                      "ratio_greens": float(num_greens / num_transactions),
#                      "ratio_blacks": float(num_blacks / num_transactions),
#                      "red_edges": red_edges,
#                      "blue_edges": blue_edges,
#                      "yellow_edges": yellow_edges,
#                      "green_edges": green_edges,
#                      "black_edges": black_edges,
#                      }
#     return render_template("userinfo.html", output=response_dict)

@app.route('/realtime')
def realtime():
    return render_template("realtime.html")


# @app.route('/api/<username>')
# def get_user(username):
#
#     # Getting an item
#     response = users_table.get(id).run(conn)
#     # response = dynamo_table.get_item(
#     #     Key={
#     #         'id': username,
#     #     }
#     # )
#     item = jsonify(response)
#     print(item)
#     return item


# def bfs(user, deg1_neighbors, color_neighbors):
#     edge_list = []
#     for deg1_neighbor in deg1_neighbors:
#         edge = sorted([int(user), int(deg1_neighbor)])
#         edge_list.append(tuple(edge))
#
#         # response = dynamo_table.get_item(Key={'id': int(deg1_neighbor)})
#         response = users_table.get(deg1_neighbor).run(conn)
#         deg2_neighbors = response[color_neighbors]
#         deg2_neighbors = set([x for x in deg2_neighbors if x is not None])
#         for deg2_neighbor in deg2_neighbors:
#             edge = sorted([int(deg1_neighbor), int(deg2_neighbor)])
#             edge_list.append(tuple(edge))
#     return set(edge_list)
