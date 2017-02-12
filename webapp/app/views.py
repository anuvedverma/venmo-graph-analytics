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
import csv
from networkx.readwrite import json_graph
import rethinkdb as r


RED = 'red'
BLUE = 'blue'
YELLOW = 'yellow'
GREEN = 'green'
BLACK = 'black'

redis_server = 'ec2-52-35-109-64.us-west-2.compute.amazonaws.com'
redis_db = redis.StrictRedis(host=redis_server, port=6379, db=0)


@app.route('/')
@app.route('/index')
def index():
    red_transitivity = float(redis_db.get(RED + '_transitivity_large'))
    blue_transitivity = float(redis_db.get(BLUE + '_transitivity_large'))
    yellow_transitivity = float(redis_db.get(YELLOW + '_transitivity_large'))
    green_transitivity = float(redis_db.get(GREEN + '_transitivity_large'))

    csv_path = str(os.path.join(app.root_path, 'static'))
    f = open(csv_path + "/transitivity.csv", 'w+')
    try:
        writer = csv.writer(f)
        writer.writerow(['transaction_type', 'transitivity'])
        writer.writerow(['Food', red_transitivity/10])
        writer.writerow(['Drinks', (blue_transitivity/10)])
        writer.writerow(['Transportation', (yellow_transitivity/10)])
        writer.writerow(['Bills', (green_transitivity/10)])
        f.flush()
    finally:
        f.close()

    num_red_transactions = redis_db.llen(RED)
    num_blue_transactions = redis_db.llen(BLUE)
    num_yellow_transactions = redis_db.llen(YELLOW)
    num_green_transactions = redis_db.llen(GREEN)

    csv_path = str(os.path.join(app.root_path, 'static'))
    f = open(csv_path + "/transaction_counts.csv", 'w+')
    try:
        writer = csv.writer(f)
        writer.writerow(['transaction_type', 'count'])
        writer.writerow(['Food', num_red_transactions])
        writer.writerow(['Drinks', num_blue_transactions])
        writer.writerow(['Transportation', num_yellow_transactions])
        writer.writerow(['Bills', num_green_transactions])
        f.flush()
    finally:
        f.close()

    return render_template("index.html")


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

    approx_transitivity = round(float(redis_db.get(color + "_transitivity")), 3)
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
    transitivity = round(nx.transitivity(graph), 3)

    remove = [edge for edge in edges if graph.degree(edge[0]) < 2 and graph.degree(edge[1]) < 2]
    graph.remove_edges_from(remove)

    response = []
    for edge in graph.edges():
        response.append({"source": edge[0], "target": edge[1]})

    print(len(response))
    return response, transitivity


# DELETE BELOW
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