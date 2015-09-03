# Benchmark Builder script
import sys, os, subprocess, datetime, argparse

now = datetime.datetime.now()
nowf = now.strftime("%Y-%m-%dT%H:%M:%SZ")

# User input
parser = argparse.ArgumentParser()
parser.add_argument("userkey", help="AWS key name.", action="store")
parser.add_argument("numnodes", help="AWS number of nodes.", action="store")
parser.add_argument("cost", help="Specify whether you want to use on demand pricing or spot bid pricing. Defaults to on demand.", choices=['spot','ondemand'], action="store", default="ondemand")
parser.add_argument("-n", "--nodetype", help="AWS instance type, m3.xlarge is default.", action="store", default="m3.xlarge")
parser.add_argument("-z", "--zone", help="AWS availability zone, us-east-1a is default.", action="store", default="us-east-1a")
parser.add_argument("type", nargs='?', help="Run the full suite of benchmarks (full) or run individual ones (partial). Will take a very long time for full. If partial please specify which benchmarks to run. Defaults to partial.", choices=['full','partial'], action="store", default="partial")
parser.add_argument("-x", "--noterminate", help="Do not automatically terminate the cluster when all steps have completed. Default behavior is to auto terminate. Use this flag to override.", action="store_true")
parser.add_argument("-ii", "--ingestimage", help="Benchmark ingest image.", action="store_true")
parser.add_argument("-rv", "--rasterizevector", help="Benchmark rasterize vector.", action="store_true")
parser.add_argument("-bp", "--buildpyramid", help="Benchmark build pyramid.", action="store_true")
parser.add_argument("-sl", "--slope", help="Benchmark slope calculation.", action="store_true")
parser.add_argument("-lc", "--landcover", help="Benchmark nested conditional statment converting land cover data to impedance values.", action="store_true")
parser.add_argument("-fs", "--frictionsurface", help="Benchmark friction surface map algebra.", action="store_true")
parser.add_argument("-l", "--launchonly", help="Spin up a MrGeo cluster with WMS server on port 8080 and leave running.", action="store_true")
args = parser.parse_args()

if args.cost == 'spot':
  cprice = subprocess.check_output('aws ec2 describe-spot-price-history --instance-types ' + args.nodetype + ' --availability-zone ' + args.zone + ' --start-time ' + nowf + ' --end-time ' + nowf + ' --product-descriptions Linux/UNIX | awk \'{print $5}\'', shell=True, stderr=subprocess.STDOUT)
  print "\nThe current spot price for your requested configuration: " + cprice
  while True:
    rprice = raw_input('Please enter your bid price: ')
    try:
      assert(float(rprice) <= 1)
      break
    except ValueError:
      print "Bid price '{}' is invalid, must be a float value.".format(rprice)
    except AssertionError:
      print "Bid price '{}' is invalid, not less than 1.0.".format(rprice)

  #if rprice >= 1:
   # raise Exception('Your spot price is too high, please try again')
else:
  spotParam = ''

# Set auto-terminate option
sg = ''
if args.noterminate:
  nt = "--no-auto-terminate"
elif args.launchonly:
  nt = "--no-auto-terminate"
  sg = ",EmrManagedMasterSecurityGroup=sg-aaa2e6cd,EmrManagedSlaveSecurityGroup=sg-43b1f524"
else:
  nt = "--auto-terminate"

# Create cluster based on user input
if args.type == 'full' or args.launchonly:
  fss = 'Name="FrictionSurfaceScratch",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/frictionSurfaceScratch.sh'
  if args.launchonly:
    fss = 'Name="WMSServer",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/wmsServer.sh'
  cmd = 'aws emr create-cluster \
	--name "MrGeo Benchmark Cluster - ' + args.userkey + ' - ' + nowf + '"' + ' \
	--ami-version 3.7 \
	--ec2-attributes KeyName=' + args.userkey + ',AvailabilityZone=' + args.zone + sg + ' \
	--log-uri "s3://mrgeo-deploy/logs" \
	--instance-groups InstanceCount=1,Name=Master,InstanceGroupType=MASTER,InstanceType=' + args.nodetype + ' InstanceCount=' + args.numnodes + ',Name=Core,InstanceGroupType=CORE,InstanceType=' + args.nodetype + spotParam + ' \
	--tags mrgeobenchmark= ' + nt + ' --no-termination-protected \
	--bootstrap-actions \
	Path=s3://mrgeo-deploy/mrgeo-emr-bootstrap.sh,Name="Fix Up EMR for MrGeo" \
	Path=s3://mrgeo-deploy/install-gdal.sh,Name="Install GDAL" \
	Path=s3://mrgeo-deploy/yarn-setup.sh,Name="Setup YARN" \
	Path=s3://support.elasticmapreduce/spark/install-spark,Name="Install Spark",Args=[-v,1.3.1.d] \
	Path=s3://elasticmapreduce/bootstrap-actions/configure-hadoop,Name="Configure YARN",Args=["-y","yarn.nodemanager.pmem-check-enabled=false","-y","yarn.nodemanager.vmem-check-enabled=false"] \
	--steps Name=InstallMrGeo,Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/mrgeo-setup.sh \
	Name="Start Spark HistoryServer",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://support.elasticmapreduce/spark/start-history-server \
	Name="Configure Spark",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=[s3://support.elasticmapreduce/spark/configure-spark.bash,spark.yarn.jar=/home/hadoop/spark/lib/spark-assembly-1.3.1-hadoop2.4.0.jar] \
	' + fss + ' --use-default-roles --enable-debugging'
  try:
  	proc = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT)
  	print "\n---------------------------------------\nSpot request successful.\nYour cluster ID is: " + proc + "Your cluster name is: MrGeo Benchmark Cluster - " + args.userkey + ' - ' + nowf + "\n---------------------------------------\nCheck cluster status by running aws emr list-instances --cluster-id " + proc
  except subprocess.CalledProcessError:
  	print "Error starting cluster!"
  	print cmd
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
	  	frictionsurface = 'Name="FrictionSurface",Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Args=s3://mrgeo-deploy/frictionsurface.sh '
	  cmd = 'aws emr create-cluster \
		--name "MrGeo Benchmark Cluster - ' + args.userkey + ' - ' + nowf + '"' + ' \
		--ami-version 3.7 \
		--ec2-attributes KeyName=' + args.userkey + ',AvailabilityZone=' + args.zone + ' \
		--log-uri "s3://mrgeo-deploy/logs" \
		--instance-groups InstanceCount=1,Name=Master,InstanceGroupType=MASTER,InstanceType=' + args.nodetype + ' InstanceCount=' + args.numnodes + ',Name=Core,InstanceGroupType=CORE,InstanceType=' + args.nodetype + spotParam + ' \
		--tags mrgeobenchmark= ' + nt + ' --no-termination-protected \
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