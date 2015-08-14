#!/bin/bash
set -e

# Script to install MapProxy, launch MrGeo webserver and seed various layers using yaml configurations in S3
sudo pip install MapProxy==1.8.0
mkdir /home/hadoop/mapproxy
aws s3 cp s3://mrgeo-deploy/mapproxy.yaml /home/hadoop/mapproxy
aws s3 cp s3://mrgeo-deploy/seed.yaml /home/hadoop/mapproxy
mrgeo webserver -p 8080 &>~/mrgeo-webserver.log &
cd /home/hadoop/mapproxy
mapproxy-util serve-develop mapproxy.yaml -b 0.0.0.0:8081 &> ./mapproxy.log &
sleep 5
mapproxy-seed -f mapproxy.yaml -c 1 seed.yaml
