#!/usr/bin/python

import boto3
import json
import sys
from datetime import datetime, timedelta
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

def get_instance_info(emr, cluster_id):
    info = emr.list_instance_groups(ClusterId=cluster_id)

    master=[]
    worker=[]
    for instance in info['InstanceGroups']:

        if instance['InstanceGroupType'] == "MASTER":
            array = master
        elif instance['InstanceGroupType'] == "CORE":
            array = worker

        array.append(instance['Status']['State'])
        array.append(instance['RequestedInstanceCount'])
        array.append(instance['RunningInstanceCount'])

    return master, worker


if len(sys.argv) != 2:
    print("usage: mrgeo-emr.py <config file>")
    sys.exit(-1)

with open(sys.argv[1]) as configFile:
    config = json.load(configFile)

emr_version=config["EmrVersion"]
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
    cluster_name = config["ClusterPrefix"] + start_time.strftime('%Y-%m-%d-%I:%M:%S') + time.tzname[1] +"-" + worker_type + "-" + `worker_nodes`

emr = boto3.client("emr")

# Define bootstrap steps - these execute on every node in the cluster
install_gdal = {
    'Name': 'Install GDAL',
    'ScriptBootstrapAction': {
        'Path': config["GDALBootstrap"],
        'Args': []
    }
}

install_opencv = {
    'Name': 'Install OpenCV',
    'ScriptBootstrapAction': {
        'Path': config["OpenCVBootstrap"],
        'Args': []
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

bootstrap_actions = [install_gdal, install_opencv]
if (install_accumulo == 1):
    bootstrap_actions.append(accumulo_bootstrap)

ec2 = boto3.client('ec2')
# Get the current spot price
if (use_spot == 1):
    use_zone = None
    spot_price = 9999
    print("Checking spot prices")
    for tryZone in zones:
        zone_name = tryZone["zone"]
        curr_time = datetime.utcnow()
        back_minute = timedelta(seconds=-59)
        minute_ago = curr_time + back_minute
        # The following call returns a list of one element that looks like:
        # [SpotPriceHistory(m3.xlarge):0.043500]
        price_result = ec2.describe_spot_price_history(StartTime=minute_ago.isoformat(),
                                                       EndTime = curr_time.isoformat(),
                                                       InstanceTypes = [worker_type],
                                                       AvailabilityZone = zone_name,
                                                       ProductDescriptions = [machine])

        if len(price_result['SpotPriceHistory']) > 0:
            zone_spot_price = float(price_result['SpotPriceHistory'][0]['SpotPrice'])
            print "  " + zone_name + ": " + "{0:.3f}".format(zone_spot_price)
            if (zone_spot_price < spot_price):
                spot_price = zone_spot_price
                use_zone = zone_name
                bid_price = spot_price * 2
    if use_zone is not None:
        print "Using zone " + use_zone + " bidding: " + "{0:.3f}".format(bid_price)
    else:
        print "No spot pricing available. Try a different instance type."
        sys.exit(-1)

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
                            ReleaseLabel=emr_version,
                            Instances={
                                'InstanceGroups': instance_groups,
                                'Ec2KeyName': key_name,
                                'KeepJobFlowAliveWhenNoSteps': True,
                                'TerminationProtected': False,
                                'Ec2SubnetId': subnet_id
                            },
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
										# "yarn.scheduler.maximum-allocation-vcores": "128",
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
                                        "spark.network.timeout": "600",
                                        "spark.driver.maxResultSize": "0",
                                        "spark.dynamicAllocation.enabled":"true"
                                    }
                                }
                            ],
                            JobFlowRole = 'EMR_EC2_DefaultRole',
                            ServiceRole = 'EMR_DefaultRole',
                            VisibleToAllUsers = True,
                            Tags = config["tags"]
                            )

cluster_id = response['JobFlowId']

cluster_info = emr.describe_cluster(ClusterId=cluster_id)
status = cluster_info['Cluster']['Status']
curr_state = "CREATION"
state_start_time = timeit.default_timer()
print(cluster_name + ' (' + cluster_id + ') - ' + start_time.strftime('%I:%M:%S %p'))

master_count = -1
worker_count = -1

while (status['State'] != 'TERMINATED' and status['State'] != 'TERMINATED_WITH_ERRORS'):
    cluster_info = emr.describe_cluster(ClusterId=cluster_id)
    master_info, worker_info = get_instance_info(emr, cluster_id)

    status = cluster_info['Cluster']['Status']
    if status['State'] != curr_state:
        curr_time = timeit.default_timer()
        print('   ' + status['State'] + ' - ' + datetime.today().strftime('%I:%M:%S %p') + ' ' + curr_state + ' took {:.0f}'.format(curr_time - state_start_time) + ' sec')
        curr_state = status['State']
        state_start_time = curr_time
    if master_count != master_info[2] or worker_count != worker_info[2]:
        print('   Master node ' + str(master_info[2]) + '/' + str(master_info[1]) + ' Worker nodes ' + str(worker_info[2]) + '/' + str(worker_info[1]))
        master_count = master_info[2]
        worker_count = worker_info[2]

    if status['State'] != 'WAITING':
        time.sleep(5) # Wait before checking status again
    else:
        time.sleep(30)

end_time = datetime.today()

total_sec = (end_time - start_time).total_seconds()
hours, remainder = divmod(total_sec, 3600)
minutes, seconds = divmod(remainder, 60)

print(cluster_name + ' ran {:.0f}:{:02.0f}:{:02.0f}'.format(hours, minutes, seconds))
