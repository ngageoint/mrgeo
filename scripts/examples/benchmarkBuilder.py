# Benchmark Builder script
import sys, os, subprocess, datetime, argparse

now = datetime.datetime.now()
nowf = now.strftime("%Y-%m-%d:%H:%M")

# User input
parser = argparse.ArgumentParser()
parser.add_argument("userkey", help="AWS key name.", action="store")
parser.add_argument("numnodes", help="AWS number of nodes.", action="store")
parser.add_argument("bidprice", help="AWS spot bid price.", action="store")
parser.add_argument("-n", "--nodetype", help="AWS instance type, m3.xlarge is default.", action="store", default="m3.xlarge")
parser.add_argument("-z", "--zone", help="AWS availability zone, us-east-1a is default.", action="store", default="us-east-1a")
parser.add_argument("scratch", help="Run the full suite of benchmarks (full) or run individual ones (partial). Will take a very long time for full. If partial please specify which benchmarks to run.", choices=['full','partial'], action="store", default="partial")
parser.add_argument("-ii", "--ingestimage", help="Benchmark MrGeo IngestImage. Use if you wish to benchmark ingest.", action="store_true")
parser.add_argument("-rv", "--rasterizevector", help="Benchmark Rasterize Vector. Use if you wish to benchmark rasterize vector.", action="store_true")
parser.add_argument("-bp", "--buildpyramid", help="MrGeo BuildPyramid. Use if you wish to benchmark building pyramids.", action="store_true")
parser.add_argument("-sl", "--slope", help="MrGeo MapAlgebra Slope. Use to benchmark slope calculation.", action="store_true")
parser.add_argument("-lc", "--landcover", help="MrGeo MapAlgebra Land Cover processing. Use to benchmark nested conditional statment.", action="store_true")
parser.add_argument("-fs", "--frictionsurface", help="MrGeo MapAlgebra friction surface processing.", action="store_true")
args = parser.parse_args()

# Create cluster based on user input
if args.scratch == 'full':
  cmd = 'aws emr create-cluster \
	--name "MrGeo Benchmark Cluster - ' + args.userkey + ' - ' + nowf + '"' + ' \
	--ami-version 3.7 \
	--ec2-attributes KeyName=' + args.userkey + ',AvailabilityZone=' + args.zone + ' \
	--log-uri "s3://mrgeo-deploy/logs" \
	--instance-groups InstanceCount=1,Name=Master,InstanceGroupType=MASTER,InstanceType=' + args.nodetype + ' InstanceCount=' + args.numnodes + ',Name=Core,InstanceGroupType=CORE,InstanceType=' + args.nodetype + ',BidPrice=' + args.bidprice + ' \
	--tags mrgeobenchmark= --auto-terminate --no-termination-protected \
	--bootstrap-actions \
	Path=s3://mrgeo-deploy/mrgeo-emr-bootstrap.sh,Name="Fix Up EMR for MrGeo" \
	Path=s3://mrgeo-deploy/install-gdal.sh,Name="Install GDAL" \
	Path=s3://mrgeo-deploy/yarn-setup.sh,Name="Setup YARN" \
	Path=s3://support.elasticmapreduce/spark/install-spark,Name="Install Spark",Args=[-v,1.3.1.d] \
	Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name="Configure YARN",Args=["-y","yarn.nodemanager.pmem-check-enabled=false","-y","yarn.nodemanager.vmem-check-enabled=false"] \
	--steps Name=InstallMrGeo,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/mrgeo-setup.sh \
	Name="Start Spark HistoryServer",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://support.elasticmapreduce/spark/start-history-server \
	Name="Configure Spark",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=[s3://support.elasticmapreduce/spark/configure-spark.bash,spark.yarn.jar=/home/hadoop/spark/lib/spark-assembly-1.3.1-hadoop2.4.0.jar] \
	Name="FrictionSurfaceScratch",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/frictionSurfaceScratch.sh \
	--use-default-roles \
	--enable-debugging'
  try:
  	proc = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  	print "\n---------------------------------------\nSpot request successful.\nYour cluster ID is: " + proc + "Your cluster name is: MrGeo Benchmark Cluster - " + args.userkey + ' - ' + nowf + "\n---------------------------------------\nCheck cluster status by running aws emr list-instances --cluster-id " + proc
  except subprocess.CalledProcessError:
  	print "Error starting cluster!"
else:
  if all( [not args.ingestimage, not args.rasterizevector, not args.buildpyramid, not args.slope, not args.landcover, not args.frictionsurface] ):
  	print "Error! Please include at least one benchmark to run if using partial option. See python benchmarkBuilder.py --help"
  else:
	  ingest = ''
	  rasterize = ''
	  pyramid = '' 
	  slope = ''
	  landcover = ''
	  frictionsurface = ''
	  if args.ingestimage:
	    ingest = 'Name="IngestImage",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/ingestImage.sh '
	  if args.rasterizevector:
	    rasterize = 'Name="RasterizeVector",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/rasterizeVector.sh '
	  if args.buildpyramid:
	    pyramid = 'Name="BuildPyramid",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/buildPyramid.sh '
	  if args.slope:
	  	slope = 'Name="Slope",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/slope.sh '
	  if args.landcover:
	  	landcover = 'Name="Landcover",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/landcover.sh '
	  if args.frictionsurface:
	  	frictionsurface = 'Name="Landcover",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/landcover.sh '
	  cmd = 'aws emr create-cluster \
		--name "MrGeo Benchmark Cluster - ' + args.userkey + ' - ' + nowf + '"' + ' \
		--ami-version 3.7 \
		--ec2-attributes KeyName=' + args.userkey + ',AvailabilityZone=' + args.zone + ' \
		--log-uri "s3://mrgeo-deploy/logs" \
		--instance-groups InstanceCount=1,Name=Master,InstanceGroupType=MASTER,InstanceType=' + args.nodetype + ' InstanceCount=' + args.numnodes + ',Name=Core,InstanceGroupType=CORE,InstanceType=' + args.nodetype + ',BidPrice=' + args.bidprice + ' \
		--tags mrgeobenchmark= --auto-terminate --no-termination-protected \
		--bootstrap-actions \
		Path=s3://mrgeo-deploy/mrgeo-emr-bootstrap.sh,Name="Fix Up EMR for MrGeo" \
		Path=s3://mrgeo-deploy/install-gdal.sh,Name="Install GDAL" \
		Path=s3://mrgeo-deploy/yarn-setup.sh,Name="Setup YARN" \
		Path=s3://support.elasticmapreduce/spark/install-spark,Name="Install Spark",Args=[-v,1.3.1.d] \
		Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name="Configure YARN",Args=["-y","yarn.nodemanager.pmem-check-enabled=false","-y","yarn.nodemanager.vmem-check-enabled=false"] \
		--steps Name=InstallMrGeo,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/mrgeo-setup.sh \
		Name="Start Spark HistoryServer",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://support.elasticmapreduce/spark/start-history-server \
		Name="Configure Spark",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=[s3://support.elasticmapreduce/spark/configure-spark.bash,spark.yarn.jar=/home/hadoop/spark/lib/spark-assembly-1.3.1-hadoop2.4.0.jar] \
		' + ingest + rasterize + pyramid + slope + landcover + frictionsurface + '--use-default-roles --enable-debugging'
	  try:
	  	proc = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
	  	print "\n---------------------------------------\nSpot request successful.\nYour cluster ID is: " + proc + "Your cluster name is: MrGeo Benchmark Cluster - " + args.userkey + ' - ' + nowf + "\n---------------------------------------\nCheck cluster status by running aws emr list-instances --cluster-id " + proc
	  except subprocess.CalledProcessError:
	  	print "Error starting cluster!"


