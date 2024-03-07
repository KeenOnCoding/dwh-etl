#!/bin/bash

echo "......................................................."
export JAVA_HOME="/etc/alternatives/java_sdk_1.8.0_openjdk"
export SPARK_HOME="/opt/spark"
echo "......................................................."
${SPARK_HOME}/bin/./spark-submit --version > spark.log  2>&1
cat /home/azuredeploy/spark.log
sudo docker version
export DOCKER_BUILDKIT=1
(cd /home/azuredeploy/dwh-etl && sudo docker build -t dwh-test -f Dockerfile.test .)
unset DOCKER_BUILDKIT
