#!/bin/bash

cd /home/azuredeploy/dwh-etl
sudo docker network create test-network
sudo docker-compose -f test/docker-compose.yaml up -d
sudo docker ps -a
sudo docker run --network test-network --rm dwh-test make test-all
