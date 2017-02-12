from __future__ import print_function # In python 2.7

from app import app
from flask import jsonify
from flask import render_template
from flask import request
from ast import literal_eval as make_tuple
import sys
import os
import json
import redis
import networkx as nx
from networkx.readwrite import json_graph
import rethinkdb as r
# import boto3


RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'
BLACK = 'black'

# conn = r.connect('localhost', 28015, db='venmo_graph_analytics_dev')
# users_table = r.table('users')

redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com'
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
    json_path = str(os.path.join(app.root_path, 'static'))
    json.dump(red_edges_response, open(json_path + "/red_graph.json", 'w+'))
    json.dump(blue_edges_response, open(json_path + "/blue_graph.json", 'w+'))
    json.dump(yellow_edges_response, open(json_path + "/yellow_graph.json", 'w+'))
    json.dump(green_edges_response, open(json_path + "/green_graph.json", 'w+'))

    # red_data = json_graph.node_link_data(red_graph)
    response_dict = {"red_edges_response": json.dumps(red_edges_response),
                     "blue_edges_response": json.dumps(blue_edges_response),
                     "yellow_edges_response": json.dumps(yellow_edges_response),
                     "green_edges_response": json.dumps(green_edges_response)}

    render_template("/redinfo.html", output=response_dict)
    render_template("/blueinfo.html", output=response_dict)
    render_template("/yellowinfo.html", output=response_dict)
    render_template("/greeninfo.html", output=response_dict)
    return render_template("/redinfo.html", output=response_dict)


@app.route('/redinfo')
def red_info():
    response = generate_response(RED)
    return render_template("/redinfo.html", output=response)


@app.route('/blueinfo')
def blue_info():
    response = generate_response(BLUE)
    return render_template("/blueinfo.html", output=response)


@app.route('/yellowinfo')
def yellow_info():
    response = generate_response(YELLOW)
    return render_template("/yellowinfo.html", output=response)


@app.route('/greeninfo')
def green_info():
    response = generate_response(GREEN)
    return render_template("/greeninfo.html", output=response)


def generate_response(color):
    edges = redis_db.lrange(color, 0, -1)
    graph_eval = process_graph(edges)
    edges_response = graph_eval[0]

    approx_transitivity = redis_db.get(color + "_transitivity")
    exact_transitivity = graph_eval[1]

    json_path = str(os.path.join(app.root_path, 'static'))
    json.dump(edges_response, open(json_path + "/" + color + "_graph.json", 'w+'))

    response_dict = {"edges_response": json.dumps(edges_response),
                     "approx_transitivity": float(approx_transitivity),
                     "exact_transitivity": float(exact_transitivity)}

    return response_dict


def process_graph(edges):
    for i in range(len(edges)):
        edges[i] = make_tuple(edges[i])

    graph = nx.Graph()
    graph.add_edges_from(edges)
    transitivity = nx.transitivity(graph)
    # transitivity = nx.average_clustering(graph)

    remove = [edge for edge in edges if graph.degree(edge[0]) < 2 and graph.degree(edge[1]) < 2]
    graph.remove_edges_from(remove)

    # cycle_edges = list(nx.find_cycle(graph, orientation='ignore'))
    # print(cycle_edges)

    response = []
    for edge in graph.edges():
        response.append({"source": edge[0], "target": edge[1]})

    print(len(response))
    return response, transitivity


@app.route('/usersearch')
def user_search():
    return render_template("usersearch.html")


@app.route('/clustertest')
def cluster_test():
    response = generate_response(BLUE)
    return render_template("/clustertest.html", output=response)



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
