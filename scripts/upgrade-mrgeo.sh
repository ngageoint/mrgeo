#!/bin/bash

usage() {
    echo "Usage: $0 [mrgeo tar.gz] <args>"
    echo "-----------------------------"
    echo "args:"
    echo "  -l  --legion            - include the legion version of mrgeo"
    echo "  -s  --tomcat-service    - tomcat is a system installed service"
    echo "                            (conf in /etc/tomcat6, logs in /var/log/tomcat6, etc)"
    echo " "
    exit 1
}

if [ `whoami` != 'root' ]; then
  echo "Please run the upgrade as root."
  exit 1
fi

set -e

[[ $# -eq 0 ]] && usage

# parse any arguments
while [ $# -gt 0 ] ; do
    case $1 in
    "-l" | "--legion")
        LEGION="true"
        shift 1
        ;;
    "-s" | "--tomcat-service")
        TOMCAT_SYSTEM="--service"
        shift 1
        ;;
     *) 
     TARBALL=$1
     shift 1
      ;;
    esac
done

#default MRGEO_HOME
if [ "$MRGEO_HOME" == "" ]; then
  export MRGEO_HOME="/usr/local/mrgeo"
fi

#default TOMCAT_HOME
if [ "$TOMCAT_HOME" == "" ]; then
  if [ -z $TOMCAT_SYSTEM ]; then
    export TOMCAT_HOME="/usr/local/tomcat"
  else
    export TOMCAT_HOME="/var/lib/tomcat6"
  fi
fi

export CWD=`pwd`

BASENAME=`echo $TARBALL | sed -e "s/.tar.gz//g" | sed -e "s/.*\\///g"`
BASEDIR=${MRGEO_HOME%/*}

echo "MRGEO_HOME:    $MRGEO_HOME"
echo "TOMCAT_HOME:   $TOMCAT_HOME"
echo "BASEDIR:       $BASEDIR"
echo "VERSIONDIR:    $BASENAME"

if [ -z $HADOOP_USER ]; then
  HADOOP_USER="hadoop"
fi
echo "HADOOP_USER: $HADOOP_USER"

if [ -z $LEGION ]; then
  echo "Using Legion:  no"
else
  echo "Using Legion:  yes"
fi

# Backup all the configuration information
if [ -a /tmp/mrgeo-conf ]; then
  rm -rf /tmp/mrgeo-conf
fi

if [ -d $MRGEO_HOME ]; then
  if [ -a $TOMCAT_HOME/webapps/mrgeo/resources/local.properties ]; then
    cp $TOMCAT_HOME/webapps/mrgeo/resources/local.properties $MRGEO_HOME/conf/ 
  fi
  cp -R $MRGEO_HOME/conf /tmp/mrgeo-conf 
fi

# Unpack the new version
cd $BASEDIR

if [ -d $BASENAME ]; then
  rm -r $BASENAME
fi

mkdir $BASENAME
cd $BASENAME
tar -xzf $CWD/$TARBALL
cd ..

# Remove the old link
if [ -d mrgeo ]; then
  rm -f mrgeo 
fi

# make a link to the new mrgeo
ln -s $BASENAME mrgeo 

# Copy over the old configuration files.
if [ -d /tmp/mrgeo-conf ]; then
  cp -R /tmp/mrgeo-conf/* $MRGEO_HOME/conf 
fi

# change ownership of hadoop, if needed
if [ -z $HADOOP_USER ]; then
  chown -R $HADOOP_USER:$HADOOP_USER $MRGEO_HOME
fi 

cd $MRGEO_HOME

if [ -z $LEGION ]; then
  FILENAME=`echo mrgeo-full-withlegion* | sed -e "s/-withlegion//g"`
  ln -s $FILENAME mrgeo-full.jar
else
  ln -s mrgeo-full-withlegion*.jar mrgeo-full.jar
fi

bash $MRGEO_HOME/bin/deploy-mrgeo.sh $TOMCAT_SYSTEM


