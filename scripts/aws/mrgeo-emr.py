#!/usr/bin/python

import boto3
import json
import sys
from datetime import datetime
from datetime import timedelta
import time
import timeit
import os
import sys

# Pretty print a JSON object - for debugging
def pretty(d, indent=0):
   for key, value in d.iteritems():
      print '\t' * indent + str(key)
      if isinstance(value, dict):
         pretty(value, indent+1)
      else:
         print '\t' * (indent+1) + str(value)

if (len(sys.argv) != 2):
  print("usage: mrgeo-emr.py <config file>")
  sys.exit(-1)

with open(sys.argv[1]) as configFile:    
    config = json.load(configFile)

use_zone=config["zone"]
zones = config["zones"]
machine= "Linux/UNIX"
master_type = config["MasterType"]
worker_type = config["WorkerType"]
worker_nodes = int(config["WorkerNodes"])
use_spot=int(config["Spot"])
install_accumulo=int(config["InstallAccumulo"])
key_name=config["Ec2KeyName"]
log_uri=config['LogUri']

start_time = datetime.today()
cluster_name = config["ClusterName"]

if (len(cluster_name) <= 0):
  cluster_name = config["ClusterPrefix"] + start_time.isoformat() + "-" + worker_type + "-" + `worker_nodes`

emr = boto3.client("emr")

# Define bootstrap steps - these execute on every node in the cluster
# Most of these bootstrap steps were used for spinning up an earlier version of
# EMR (3.7). They are retained here in case we need to go back to that
# earlier version. Note that they are referenced in some commented out code
# for that reason.
fix_emr_for_mrgeo = {
  'Name': 'Fixup EMR for MrGeo',
  'ScriptBootstrapAction': {
    'Path': 's3://mrgeo-dg/deploy/mrgeo-emr-bootstrap.sh',
    'Args': []
  }
}

install_gdal = {
  'Name': 'Install GDAL',
  'ScriptBootstrapAction': {
    'Path': config["GDALBootstrap"],
    'Args': []
  }
}

setup_yarn = {
  'Name': 'Setup YARN',
  'ScriptBootstrapAction': {
    'Path': 's3://mrgeo-dg/deploy/yarn-setup.sh',
    'Args': []
  }
}

install_spark = {
  'Name': 'Install Spark',
  'ScriptBootstrapAction': {
    'Path': 's3://support.elasticmapreduce/spark/install-spark',
    'Args': ["-l","WARN","-v","1.3.1.d"]
  }
}

configure_yarn = {
  'Name': 'Configure YARN',
  'ScriptBootstrapAction': {
    'Path': 's3://elasticmapreduce/bootstrap-actions/configure-hadoop',
    'Args': ["-y","yarn.nodemanager.pmem-check-enabled=false",
             "-y","yarn.nodemanager.vmem-check-enabled=false",
             "-y","yarn.log-aggregation-enable=true"]
  }
}

accumulo_bootstrap = {
  'Name': 'Install Accumulo',
  'ScriptBootstrapAction': {
    'Path': 's3://elasticmapreduce.bootstrapactions/accumulo/1.6.1/install-accumulo_mj',
    'Args': []
  }
}

# Define setup steps - these execute only on the name node
install_mrgeo_step = {
  'Name': 'Install MrGeo',
  'ActionOnFailure': 'TERMINATE_CLUSTER',
  'HadoopJarStep': {
    'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
    'Args': [config["InstallMrGeoStep"]]
  }
}

start_spark_history = {
  'Name': "Start Spark HistoryServer",
  'ActionOnFailure': 'TERMINATE_CLUSTER',
  'HadoopJarStep': {
    'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
    'Args': ["s3://support.elasticmapreduce/spark/start-history-server"]
  }
}
configure_spark = {
  'Name': "Configure Spark",
  'ActionOnFailure': 'TERMINATE_CLUSTER',
  'HadoopJarStep': {
    'Jar': 's3://us-west-2.elasticmapreduce/libs/script-runner/script-runner.jar',
    'Args': ["s3://support.elasticmapreduce/spark/configure-spark.bash", "spark.yarn.jar=/home/hadoop/spark/lib/spark-assembly-1.3.1-hadoop2.4.0.jar"]
  }
}

#bootstrap_actions = [fix_emr_for_mrgeo, install_gdal, setup_yarn, install_spark, configure_yarn]
bootstrap_actions = [install_gdal]
#bootstrap_actions = []
if (install_accumulo == 1):
  bootstrap_actions.append(accumulo_bootstrap)

ec2 = boto3.client('ec2')
# Get the current spot price
if (use_spot == 1):
  spot_price = 9999
  for tryZone in zones:
    zone_name = tryZone["zone"]
    curr_time = datetime.utcnow()
    back_minute = timedelta(seconds=-59)
    minute_ago = curr_time + back_minute
    print curr_time.isoformat()
    # The following call returns a list of one element that looks like:
    # [SpotPriceHistory(m3.xlarge):0.043500]
    price_result = ec2.describe_spot_price_history(StartTime=minute_ago.isoformat(),
                                                   EndTime = curr_time.isoformat(),
                                                   InstanceTypes = [worker_type],
                                                   AvailabilityZone = zone_name,
                                                   ProductDescriptions = [machine])

    zone_spot_price = float(price_result['SpotPriceHistory'][0]['SpotPrice'])
    print "Price for zone " + zone_name + " is " + "{0:.3f}".format(zone_spot_price)
    if (zone_spot_price < spot_price):
      spot_price = zone_spot_price
      use_zone = zone_name
      bid_price = spot_price * 2
  print "Using zone " + use_zone
  print "Using bid price: " + "{0:.3f}".format(bid_price)

instance_groups = []
instance_groups.append({
    'InstanceCount': 1,
    'InstanceRole': 'MASTER',
    'InstanceType': master_type,
    'Market': 'ON_DEMAND',
    'Name': 'Main node'
    })
if (use_spot == 1):
  instance_groups.append({
      'InstanceCount': worker_nodes,
      'InstanceRole': 'CORE',
      'InstanceType': worker_type,
      'Market': 'SPOT',
      'BidPrice': '{0:.3f}'.format(bid_price),
      'Name': 'Worker nodes'
  })
else:
  instance_groups.append({
      'InstanceCount': worker_nodes,
      'InstanceRole': 'CORE',
      'InstanceType': worker_type,
      'Market': 'ON_DEMAND',
      'Name': 'Worker nodes'
  })

subnet_id = ""
for z in zones:
  if (z["zone"] == use_zone):
    subnet_id = z["subnetId"]

response = emr.run_job_flow(Name=cluster_name,
                            LogUri=log_uri,
                            ReleaseLabel='emr-4.3.0',
#                 AmiVersion='3.10',
                 Instances={
                   'InstanceGroups': instance_groups,
                   'Ec2KeyName': key_name,
#                   'Placement': {
#                     'AvailabilityZone': ZONE
#                   },
                   'KeepJobFlowAliveWhenNoSteps': True,
                   'TerminationProtected': False,
                   'Ec2SubnetId': subnet_id
                 },
#                 Steps=[install_mrgeo_step, start_spark_history, configure_spark],
                 Steps=[install_mrgeo_step],
                            BootstrapActions = bootstrap_actions,
                            Applications=[
#                   {
#                     "Name": "Hadoop"
#                   }
#                   ,
                   {
                     "Name": "Spark"
                   }
                 ],
                            Configurations=[
                   {
                     "Classification": "yarn-site",
                     "Properties": {
                       "yarn.nodemanager.pmem-check-enabled": "false",
                       "yarn.nodemanager.vmem-check-enabled": "false",
                       "yarn.scheduler.minimum-allocation-mb": "1024",
                       "yarn.app.mapreduce.am.command-opts": "-Xmx820m",
                       "yarn.nodemanager.aux-services": "spark_shuffle",
                       "yarn.nodemanager.aux-services.spark_shuffle.class": "org.apache.spark.network.yarn.YarnShuffleService"
                     }
                   },
                   {
                     "Classification": "mapred-site",
                     "Properties": {
                       "yarn.app.mapreduce.am.resource.mb" : "1024"
                     }
                   },
#                   {
#                     "Classification": "core-site",
#                     "Properties": {
#                     }
#                   },
                   {
                     "Classification": "spark-defaults",
                     "Properties": {
                       "spark.yarn.jar": "/usr/lib/spark/lib/spark-assembly.jar",
                       "spark.network.timeout": "600"
                     }
                   }
                 ],
                            JobFlowRole = 'EMR_EC2_DefaultRole',
                            ServiceRole = 'EMR_DefaultRole',
                            VisibleToAllUsers = True,
                            Tags = config["tags"]
                            )

cluster_id = response['JobFlowId']

print("Cluster id: " + cluster_id)
cluster_info = emr.describe_cluster(ClusterId=cluster_id)
status = cluster_info['Cluster']['Status']
curr_state = status['State']
state_start_time = timeit.default_timer()
print(cluster_name + ' with id ' + cluster_id + ' is ' + status['State'] + ' at ' + `start_time`)
while (status['State'] != 'TERMINATED' and status['State'] != 'TERMINATED_WITH_ERRORS'):
  time.sleep(30) # Wait before checking status again
  cluster_info = emr.describe_cluster(ClusterId=cluster_id)
  status = cluster_info['Cluster']['Status']
  if (status['State'] != curr_state):
    curr_time = timeit.default_timer()
    print(cluster_name + ' with id ' + cluster_id + ' took ' + `(curr_time - state_start_time)` + ' in state ' + curr_state)
    print(cluster_name + ' with id ' + cluster_id + ' is ' + status['State'] + ' at ' + `curr_time`)
    curr_state = status['State']
    state_start_time = curr_time
  
end_time = timeit.default_timer()
print(cluster_name + ' took ' + str(end_time - start_time))
