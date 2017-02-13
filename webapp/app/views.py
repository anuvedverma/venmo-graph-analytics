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


# Homepage for large-scale Venmo community analysis
@app.route('/')
@app.route('/index')
def index():

    # Get approx. transitivity for all 2017 venmo data
    red_transitivity = float(redis_db.get(RED + '_transitivity_large'))
    blue_transitivity = float(redis_db.get(BLUE + '_transitivity_large'))
    yellow_transitivity = float(redis_db.get(YELLOW + '_transitivity_large'))
    green_transitivity = float(redis_db.get(GREEN + '_transitivity_large'))

    # Write to local CSV file for D3 visualization
    csv_path = str(os.path.join(app.root_path, 'static'))
    f = open(csv_path + "/transitivity.csv", 'w+')
    try:
        writer = csv.writer(f)
        writer.writerow(['transaction_type', 'transitivity'])
        writer.writerow(['Food', red_transitivity])
        writer.writerow(['Drinks', (blue_transitivity)])
        writer.writerow(['Transportation', (yellow_transitivity)])
        writer.writerow(['Bills', (green_transitivity)])
        f.flush()
    finally:
        f.close()

    # Get ratios of transaction types for all 2017 venmo data
    num_red_transactions = float(redis_db.get('num_' + RED + '_large'))
    num_blue_transactions = float(redis_db.get('num_' + BLUE + '_large'))
    num_yellow_transactions = float(redis_db.get('num_' + YELLOW + '_large'))
    num_green_transactions = float(redis_db.get('num_' + GREEN + '_large'))

    num_caught = num_red_transactions + num_blue_transactions + num_yellow_transactions + num_green_transactions
    percent_caught = float(redis_db.get('percent_caught'))

    num_tot_transactions = num_caught / percent_caught

    # Write to local CSV file for D3 visualization
    csv_path = str(os.path.join(app.root_path, 'static'))
    f = open(csv_path + "/transaction_ratios.csv", 'w+')
    try:
        writer = csv.writer(f)
        writer.writerow(['transaction_type', 'ratio'])
        writer.writerow(['Food', num_red_transactions / num_tot_transactions])
        writer.writerow(['Drinks', num_blue_transactions / num_tot_transactions])
        writer.writerow(['Transportation', num_yellow_transactions / num_tot_transactions])
        writer.writerow(['Bills', num_green_transactions / num_tot_transactions])
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