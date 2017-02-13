#!/bin/bash

PEG_ROOT=$PEGASUS_HOME

CLUSTER_NAME=anuvedverma-spark-cluster

peg up spark-master.yml &
peg up spark-workers.yml &

wait

peg fetch ${CLUSTER_NAME}

peg install ${CLUSTER_NAME} ssh
peg install ${CLUSTER_NAME} aws
peg install ${CLUSTER_NAME} hadoop
peg install ${CLUSTER_NAME} spark