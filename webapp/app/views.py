from __future__ import print_function # In python 2.7

from app import app
from flask import jsonify
from flask import render_template
from flask import request

import sys
import json
import boto3

dynamodb = boto3.resource('dynamodb',
                          aws_access_key_id='dummy-access-id',
                          aws_secret_access_key='dummy-secret-access-key',
                          region_name='us-west-2',
                          endpoint_url='http://localhost:8000')  # Set DynamoDB connection (local)

dynamo_table = dynamodb.Table('venmo-graph-analytics-test')  # Set DynamoDB table


@app.route('/')
@app.route('/index')
def index():
    user = {'nickname': 'Miguel'}  # fake user
    mylist = [1, 2, 3, 4]
    # return render_template("index.html", title='Home', list=mylist, )
    return render_template("index.html", title='Home', user=user)

@app.route('/usersearch')
def usersearch():
    return render_template("usersearch.html")

@app.route("/usersearch", methods=['POST'])
def username_post():
    username = request.form["username"]
    print("Received request: " + username)

    response = dynamo_table.get_item(
        Key={
            'username': username,
        }
    )

    # print("Query result: " + response)
    json_response = {"name":response['Item']['name'], "username":response['Item']['username'], "message":response['Item']['message']}
    return render_template("userinfo.html", output=json_response)

@app.route('/realtime')
def realtime():
    return render_template("realtime.html")


@app.route('/api/<username>')
def get_user(username):

    # Getting an item
    response = dynamo_table.get_item(
        Key={
            'username': username,
        }
    )
    item = jsonify(response['Item'])
    print(item)
    return item