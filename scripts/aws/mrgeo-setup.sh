#!/bin/bash
set -e
aws s3 cp s3://mrgeodev/mrgeo-0.5.0-2.4.0-SNAPSHOT-2.4.0.tar.gz /home/hadoop
mkdir /home/hadoop/mrgeo
cd /home/hadoop/mrgeo
tar -xvf /home/hadoop/mrgeo-0.5.0-2.4.0-SNAPSHOT-2.4.0.tar.gz
export MRGEO_COMON_HOME=/home/hadoop/mrgeo
hadoop fs -mkdir /mrgeo
hadoop fs -mkdir /mrgeo/images
hadoop fs -mkdir /mrgeo/images/color-scales
hadoop fs -put /home/hadoop/mrgeo/color-scales/* /mrgeo/color-scales

