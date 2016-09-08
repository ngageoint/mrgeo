#!/bin/bash

# find the mrgeo tar.  For now, we'll just take the 1st instance,  we
# should probably take the latest version
S3_BASE="s3://mrgeo-deploy/"
VERSION="emr4.5.0"

FILES=`aws s3 ls $S3_BASE | awk '{print $4}' | grep '^mrgeo' | grep '\.tar\.gz$' | grep $VERSION`

for FILE in $FILES; do
  echo $FILE
  MRGEO_JAR=$FILE
  break
done

HOME=/home/hadoop

# Download the mrgeo tar
aws s3 cp "$S3_BASE""$MRGEO_JAR" $HOME

MRGEO_HOME="$HOME"/mrgeo

if [ -d $MRGEO_HOME ]; then
  rm -r $MRGEO_HOME/*
else
  mkdir $MRGEO_HOME
fi

cd $MRGEO_HOME

# unpack the tar
tar -xvf "$HOME"/$MRGEO_JAR

# copy the conf file
aws s3 cp s3://mrgeo-deploy/mrgeo.conf /home/hadoop/mrgeo/conf

export MRGEO_COMMON_HOME="$MRGEO_HOME"
export MRGEO_CONF_DIR="$MRGEO_HOME"/conf

# create some HDFS directories
hadoop fs -mkdir /mrgeo
hadoop fs -mkdir /mrgeo/images
hadoop fs -put /home/hadoop/mrgeo/color-scales /mrgeo

# Create a MrGeo profile script, setting some common env variables
PROFILE=/etc/profile.d/mrgeo.sh
sudo sh -c "echo -e '# MrGeo' > $PROFILE"
sudo sh -c "echo -e 'export MRGEO_COMMON_HOME=$MRGEO_COMMON_HOME' >> $PROFILE"
sudo sh -c "echo -e 'export MRGEO_CONF_DIR=$MRGEO_CONF_DIR' >> $PROFILE"
sudo sh -c "echo -e 'export PATH=\$PATH:$MRGEO_HOME/bin' >> $PROFILE"

# Create a spark profile script
PROFILE=/etc/profile.d/spark.sh
sudo sh -c "echo -e '# Spark' > $PROFILE"
sudo sh -c "echo -e 'export SPARK_HOME=/usr/lib/spark' >> $PROFILE"
exit 0
