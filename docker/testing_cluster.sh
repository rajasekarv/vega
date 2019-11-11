#!/bin/bash
SCRIPT_PATH=`dirname $(readlink -f $0)`
cd SCRIPT_PATH

# Deploy a testing cluster with one master and 3 workers
docker-compose up -d
