#!/usr/bin/env bash

# custom startup log
logfile=/mnt/startup.log
exec > $logfile 2>&1

# TODO: these are all duplicate settings; need to be removed
USE_HDFS=false
S3_BUCKET=canvasspadac

# not sure why FABRIC_HOME isn't automatically recognized here...
FABRIC_HOME=/usr/local/appistry/cloudiq/system
NETWORK_BRIDGE_HOME=/usr/local/appistry/networkbridge
CLOUDIQ_USERNAME=fabric-admin
CLOUDIQ_PASSWORD=fabric-admin
NETWORK_BRIDGE_PORT=16081
MANAGEMENT_PORT=16080

AWS_ACCESS_KEY_ID=
export AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=
export AWS_SECRET_ACCESS_KEY

date
echo "USE_HDFS: $USE_HDFS"
echo
echo "AWS_ACCESS_KEY_ID: $AWS_ACCESS_KEY_ID"
echo
echo "AWS_SECRET_ACCESS_KEY: $AWS_SECRET_ACCESS_KEY"
echo
echo "S3_BUCKET: $S3_BUCKET"
echo
if [ "$USE_HDFS" == "false" ]; then
  echo "CloudIQ fabric home: $FABRIC_HOME"
  echo
  echo "CloudIQ username: $CLOUDIQ_USERNAME"
  echo
  echo "CloudIQ password: $CLOUDIQ_PASSWORD"
  echo
  echo "CloudIQ network bridge port: $NETWORK_BRIDGE_PORT"
  echo
  echo "CloudIQ management port: $MANAGEMENT_PORT"
  echo
fi

# Slaves are started after the master, and are told its address by sending a
# modified copy of this file which sets the MASTER_HOST variable. 
# A node  knows if it is the master or not by inspecting the security group
# name. If it is the master, then it retrieves its address using instance data.
MASTER_HOST=%MASTER_HOST%  # Interpolated before being sent to EC2 node
SECURITY_GROUPS=`wget -q -O - http://169.254.169.254/latest/meta-data/security-groups`
IS_MASTER=`echo $SECURITY_GROUPS | awk '{ a = match ($0, "-master$"); if (a) print "true"; else print "false"; }'`
if [ "$IS_MASTER" == "true" ]; then
 MASTER_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
else
 SLAVE_HOST=`wget -q -O - http://169.254.169.254/latest/meta-data/local-hostname`
fi

date
if [ "$IS_MASTER" == "true" ]; then
  echo "Master host: $MASTER_HOST"
else
  echo "Slave host: $SLAVE_HOST"
fi
echo

HADOOP_HOME=`ls -d /usr/local/hadoop-*`
TOMCAT_HOME=`ls -d /usr/local/*tomcat*`

date
echo "Creating Hadoop config files..."
echo

# Hadoop configuration

if [ "$USE_HDFS" == "true" ]; then

cat > $HADOOP_HOME/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
  <name>fs.default.name</name>
  <value>hdfs://$MASTER_HOST:9000</value>
</property>

<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
</property>

<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>

<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>

</configuration>

EOF

else

cat > $HADOOP_HOME/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
  <name>fs.default.name</name>
  <value>abs:/</value>
</property>

<property>
  <name>hadoop.tmp.dir</name>
  <value>/mnt/hadoop</value>
</property>

<property>
  <name>fs.s3n.awsAccessKeyId</name>
  <value>$AWS_ACCESS_KEY_ID</value>
</property>

<property>
  <name>fs.s3n.awsSecretAccessKey</name>
  <value>$AWS_SECRET_ACCESS_KEY</value>
</property>

<property>
  <name>fs.storage.impl</name>
  <value>org.apache.hadoop.fs.appistry.FabricStorageFileSystem</value>
  <description>Appistry Fabric Storage FS for storage: uris.</description>
</property>

<property>
  <name>fs.abs.impl</name>
  <value>org.apache.hadoop.fs.appistry.BlockedFabricStorageFileSystem</value>
  <description>Block-aware Appistry Fabric Storage FS for storage: uris.</description>
</property>

<!-- unlike HDFS, this cannot exceed the number of machines available for copying without causing an error -->
<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>

</configuration>

EOF

fi

# disabling this for now, since its not used by benchmarking
# HOSTS_EXCLUDE_FILE=$HADOOP_HOME/conf/exclude

if [ "$USE_HDFS" == "true" ]; then

cat > $HADOOP_HOME/conf/hdfs-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
  <name>dfs.replication</name>
  <value>3</value>
</property>
	
<property>
  <name>dfs.umask</name>
  <value>002</value>
</property>

<property>
  <name>dfs.balance.bandwidthPerSec</name>
  <value>104857600</value>
  <description>
        Specifies the maximum amount of bandwidth that each datanode
        can utilize for the balancing purpose in term of
        the number of bytes per second.
  </description>
</property>

</configuration>
EOF

else

# don't need this config file when using CloudIQ
if [ -f $HADOOP_HOME/conf/hdfs-site.xml ]; then
  rm $HADOOP_HOME/conf/hdfs-site.xml
fi

cat > $HADOOP_HOME/conf/appistry-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
  <name>fs.appistry.storage.host</name>
  <value>localhost</value>
</property>

# in bytes; default size is 128 MB
<property>
  <name>fs.appistry.block.size</name>
  <value>67108864</value>
</property>

<property>
  <name>fs.appistry.chunked</name>
  <value>true</value>
</property>

<property>
  <name>fs.appistry.storage.logger.enabled</name>
  <value>false</value>
</property>

<property>
  <name>fs.appistry.hostnames.enabled</name>
  <value>true</value>
</property>

</configuration>
EOF

fi

if [ "$USE_HDFS" == "true" ]; then

cat > $HADOOP_HOME/conf/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<!-- make this about 10 * # of data node cores -->
<property>
  <name>mapred.map.tasks</name>
  <value>100</value>
</property>

<!-- make this the # of data nodes * .95 -->
<property>
  <name>mapred.reduce.tasks</name>
  <value>5</value>
</property>

<property>
  <name>mapred.child.java.opts</name>
  <value>-Xmx1024m</value>
</property>
	
<property>
  <name>mapred.job.tracker</name>
  <value>hdfs://$MASTER_HOST:9001</value>
</property>

<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>2</value>
</property>

<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>2</value>
</property>

<property>
  <name>mapred.map.max.attempts</name>
  <value>9</value>
</property>

<property>
  <name>mapred.reduce.max.attempts</name>
  <value>9</value>
</property>

<property>
  <name>mapred.task.timeout</name>
  <value>7200000</value>
</property>

</configuration>
EOF

else

cat > $HADOOP_HOME/conf/mapred-site.xml <<EOF
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>

<!-- Put site-specific property overrides in this file. -->

<configuration>

<!-- make this about 10 * # of data node cores -->
<property>
  <name>mapred.map.tasks</name>
  <value>1280</value>
</property>

<!-- make this the # of data nodes * .95 -->
<property>
  <name>mapred.reduce.tasks</name>
  <value>61</value>
</property>

<property>
  <name>mapred.child.java.opts</name>
  <value>-Xmx1024m</value>
</property>

<property>
  <name>mapred.job.tracker</name>
  <value>http://$MASTER_HOST:9001</value>
</property>

<property>
  <name>mapred.tasktracker.map.tasks.maximum</name>
  <value>2</value>
</property>

<property>
  <name>mapred.tasktracker.reduce.tasks.maximum</name>
  <value>2</value>
</property>

<property>
  <name>mapred.map.max.attempts</name>
  <value>9</value>
</property>

<property>
  <name>mapred.reduce.max.attempts</name>
  <value>9</value>
</property>

<property>
  <name>mapred.task.timeout</name>
  <value>7200000</value>
</property>

</configuration>
EOF

fi

source /root/.bash_profile
sleep 5

# CloudIQ configuration

if [ "$USE_HDFS" == "false" ]; then

  CLOUDIQ_CREDS=$CLOUDIQ_USERNAME:$CLOUDIQ_PASSWORD
  MANAGEMENT_URI=http://$CLOUDIQ_CREDS@localhost:$MANAGEMENT_PORT/files
  NETWORK_BRIDGE_URI=http://$CLOUDIQ_CREDS@localhost:$NETWORK_BRIDGE_PORT/bridge/hints  
  
  rm $NETWORK_BRIDGE_HOME/bridge_members.dat
  rm $NETWORK_BRIDGE_HOME/bridge_hints.dat

  date
  echo "Adding bridge hint to $NETWORK_BRIDGE_URI/$MASTER_HOST ..."
  echo
  i=0
  while (( ++i <= 5 ))
  do
    curl -X PUT $NETWORK_BRIDGE_URI/$MASTER_HOST
    date
    if [[ -s $NETWORK_BRIDGE_HOME/bridge_hints.dat ]] ; then
      echo "network bridge hints:"
      echo
      cat $NETWORK_BRIDGE_HOME/bridge_hints.dat
      echo
      break;
    else
      echo "trouble adding bridge hint.  trying again..."
    fi
    sleep 5
  done

  if [[ ! -s $NETWORK_BRIDGE_HOME/bridge_hints.dat ]] ; then
    echo "ERROR: Unable to add network bridge hints."
    echo
  fi

  mkdir /mnt/storage_repo
  chmod 777 /mnt/storage_repo

  # copy CloudIQ Hadoop storage driver - can't use hadoop fs here, since hadoop depends on this jar
  # use s3 get rather than wget, since this data isn't readable by everyone
  date
  echo "Copying CloudIQ storage driver..."
  echo
  cd /usr/local/s3sync
  ./s3cmd.rb get $S3_BUCKET:fsfs.jar $HADOOP_HOME/lib/fsfs.jar
  chmod +x $HADOOP_HOME/lib/fsfs.jar

  date
  echo "Deploying CloudIQ license to $MANAGEMENT_URI/license.cfg ..."
  echo
  cd /usr/local/s3sync
  ./s3cmd.rb get $S3_BUCKET:license.cfg $FABRIC_HOME/license.cfg
  # assumes license file is version 1.0 or higher...if not, need to change as is done with the storage config
  curl -X PUT $MANAGEMENT_URI/license.cfg -T $FABRIC_HOME/license.cfg

  service fabric_keeper restart

fi

date
echo "Starting Hadoop..."
echo

[ ! -f /etc/hosts ] &&  echo "127.0.0.1 localhost" > /etc/hosts

mkdir -p /mnt/hadoop/logs

# not set on boot
export USER="root"

# cat > $HOSTS_EXCLUDE_FILE <<EOF
#
# EOF

if [ "$IS_MASTER" == "true" ]; then

  # CloudIQ doesn't need the namenode
  if [ "$USE_HDFS" == "true" ]; then
    # only format on first boot
    [ ! -e /mnt/hadoop/dfs ] && "$HADOOP_HOME"/bin/hadoop namenode -format  
    "$HADOOP_HOME"/bin/hadoop-daemon.sh start namenode
  fi
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start jobtracker
  # run a tasktracker on the CloudIQ master too
  if [ "$USE_HDFS" == "false" ]; then
    "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker
  fi
 
else

  # CloudIQ doesn't need the datanode
  if [ "$USE_HDFS" == "true" ]; then
    "$HADOOP_HOME"/bin/hadoop-daemon.sh start datanode
  fi
  "$HADOOP_HOME"/bin/hadoop-daemon.sh start tasktracker

fi
echo

# Run this script on next boot
rm -f /var/ec2/ec2-run-user-data.*

# disabling this for now, since its not used by benchmarking
# cat > $HOSTS_EXCLUDE_FILE <<EOF
# 
# EOF

source /root/.bash_profile
sleep 5

# Hadoop should auto-copy these jars to all slaves, right?
if [ "$IS_MASTER" == "true" ]; then

date
echo "Deploying custom application..."
echo

export MRGEO_HOME=/usr/local/mrgeo
mkdir $MRGEO_HOME
mkdir $MRGEO_HOME/bin
mkdir $MRGEO_HOME/conf

hadoop fs -copyToLocal s3n://$S3_BUCKET/$mrgeo-core-full-SNAPSHOT-r1.jar $MRGEO_HOME/bin

date
echo "Configuring MrGeo..."
echo
cat > $MRGEO_HOME/conf/mrgeo.conf <<EOF
jar.path = $MRGEO_HOME/bin/$mrgeo-core-full-SNAPSHOT-r1.jar
EOF

cat > $MRGEO_HOME/bin/mrgeo <<EOF
#!/bin/sh
hadoop jar $MRGEO_HOME/bin/$mrgeo-core-full-SNAPSHOT-r1.jar org.mrgeo.cmd.\$*
EOF
chmod +x $MRGEO_HOME/bin/mrgeo

cat >> /etc/profile <<EOF
export MRGEO_HOME=$MRGEO_HOME
export PATH=\$PATH:\$MRGEO_HOME/bin
EOF

fi