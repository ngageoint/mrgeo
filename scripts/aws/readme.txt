OVERVIEW

This directory contains an AWS script that can be used to create an Elastic
Map Reduce cluster within AWS with MrGeo pre-installed onto the master node
of the cluster. The script defines variables that users can change to modify
various aspects of the cluster configuration, such as number and/or type of
nodes, tags and other settings. It assumes that the user has run "aws configure"
already.

Once the cluster is created, the user can ssh to the master node, set the
MRGEO_HOME environment variable and execute map algebra commands from the
command line using something like the following:

Note: The SSH command to use is available from within the Amazon Manager
Once ssh'ed to the master node:

export MRGEO_COMMON_HOME=/home/haoop/mrgeo
export MRGEO_CONF_DIR=/home/haoop/mrgeo/conf
cd mrgeo
bin/mrgeo mapalgebra -e "put your map algebra here" -o /mrgeo/images/myoutput

The user can also run a standalone web server for MrGeo services using:
bin/mrgeo webserver

Once the web server is running, and the user follows the instructions provided
in the Amazon Manager for setting up a proxy on their local machine, they can
use software like QGIS (through the proxy running on their local machine) to
make WMS requests to the MrGeo web server running on the master node.


DETAILS ABOUT THE SCRIPTS IN THIS DIRECTORY

The mrgeo-setup.sh script is used as a "step" when configuring a MrGeo cluster
in AWS. Steps are executed only on the master node, and since we only want MrGeo
installed on the master node (not the data nodes), this is the best way to perform
the MrGeo installation on an EMR cluster.

The mrgeo-emr-bootstrap.sh script is executed as a bootstrap process while spinning
up a cluster for MrGeo. The commands in that script are executed on all nodes in
the cluster, so it should not have any specific setup steps for the MrGeo install
image because that would not apply to data nodes. This should be used for cluster
configuration across all nodes in the cluster.

Both of these scripts should be copied to s3://mrgeodev prior to running the AWS CLI
to spin up a MrGeo cluster because the scripts for running the AWS CLI include
references to those scripts in s3://mrgeodev so they can be executed.

