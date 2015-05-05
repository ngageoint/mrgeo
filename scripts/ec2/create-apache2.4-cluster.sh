#!/bin/bash
#
# Modify these settings as needed.
#

# The name for the key pair to use in AWS to launch the cluster
KEY_NAME=dave.johnson
# The instance type to use for the master node
MASTER_INSTANCE_TYPE=m3.xlarge
# The instance type to use for the data nodes
DATA_INSTANCE_TYPE=m1.large
# The number of data nodes to use
NUM_DATA_NODES=4

#
# Settings that should not change much and use the settings above
#
AMI_VERSION=3.7
STEPS="Name=InstallMrGeo,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeodev/mrgeo-setup.sh"
CLUSTER_NAME="MrGeo Cluster"
EC2_ATTRIBUTES="KeyName=${KEY_NAME}"
INSTANCE_GROUPS="InstanceCount=1,Name=Master,InstanceGroupType=MASTER,InstanceType=${MASTER_INSTANCE_TYPE} InstanceCount=${NUM_DATA_NODES},Name=Core,InstanceGroupType=CORE,InstanceType=${DATA_INSTANCE_TYPE}"
BOOTSTRAP_ACTIONS=Path=s3://mrgeodev/mrgeo-emr-bootstrap.sh,Name='Fix Up EMR for MrGeo'
LOG_URI="s3://mrgeodev/logs"
TAGS="mrgeo="

#echo "aws emr create-cluster --name ${CLUSTER_NAME} --ami-version ${AMI_VERSION} --ec2-attributes ${EC2_ATTRIBUTES} --bootstrap-actions ${BOOTSTRAP_ACTIONS} --steps ${STEPS} --log-uri ${LOG_URI} --instance-groups ${INSTANCE_GROUPS} --tags ${TAGS} --no-auto-terminate --no-termination-protected"
aws emr create-cluster --name "${CLUSTER_NAME}" --ami-version "${AMI_VERSION}" --ec2-attributes "${EC2_ATTRIBUTES}" --bootstrap-actions "${BOOTSTRAP_ACTIONS}" --steps "${STEPS}" --log-uri "${LOG_URI}" --instance-groups ${INSTANCE_GROUPS} --tags "${TAGS}" --no-auto-terminate --no-termination-protected
