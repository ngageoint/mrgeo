#!/bin/bash
set -e
aws s3 cp s3://mrgeo-deploy/mrgeo-1.1.0-2.7.1-SNAPSHOT-2.7.1.tar.gz /home/hadoop
mkdir /home/hadoop/mrgeo
cd /home/hadoop/mrgeo
tar -xvf /home/hadoop/mrgeo-1.1.0-2.7.1-SNAPSHOT-2.4.0.tar.gz
aws s3 cp s3://mrgeo-deploy/mrgeo.conf /home/hadoop/mrgeo/conf
export MRGEO_HOME=/home/hadoop/mrgeo
hadoop fs -mkdir /mrgeo
hadoop fs -mkdir /mrgeo/images
hadoop fs -put /home/hadoop/mrgeo/color-scales /mrgeo

PROFILE=/etc/profile.d/mrgeo.sh

sudo sh -c "echo -e '# MrGeo' > $PROFILE"
sudo sh -c "echo -e 'export MRGEO_HOME=/home/hadoop/mrgeo' >> $PROFILE"
sudo sh -c "echo -e 'export PATH=\$PATH:/home/hadoop/mrgeo/bin' >> $PROFILE"

PROFILE=/etc/profile.d/spark.sh
sudo sh -c "echo -e '# Spark' > $PROFILE"
sudo sh -c "echo -e 'export SPARK_HOME=/usr/lib/spark' >> $PROFILE"

