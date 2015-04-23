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

