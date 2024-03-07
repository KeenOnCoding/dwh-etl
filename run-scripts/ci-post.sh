#!/bin/bash

cd /home/azuredeploy/dwh-etl
sudo docker-compose -f test/docker-compose.yaml down
sudo docker prune -f
sudo docker network rm test-network

