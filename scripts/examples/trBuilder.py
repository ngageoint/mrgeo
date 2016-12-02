# Terrain Builder script
import argparse
import datetime
import subprocess

now = datetime.datetime.now()
nowf = now.strftime("%Y%m%d-%s")

# User input
parser = argparse.ArgumentParser()
parser.add_argument("terrainFunction", help="Specify terrain operation.", choices=['slope', 'aspect'], action="store")
parser.add_argument("inputElevation",
                    help="S3 or HDFS location for input MrsPyramid elevation layer. Defaults to Aster GDEM.",
                    action="store", default="s3://mrgeo/images/aster-30m")
parser.add_argument("-u", "--units", help="Specify slope/aspect units in either degree or radians.",
                    choices=['deg', 'rad'], action="store", default="rad")
parser.add_argument("-o", "--output", help="S3 or HDFS location for results.", action="store",
                    default="s3://mrgeo/images/trout-")
args = parser.parse_args()

# Build mrgeo command from user input
outLoc = args.output + args.terrainFunction + "-" + nowf
cmd = "mrgeo mapalgebra -e \"result = " + args.terrainFunction + "([" + args.inputElevation + "], \"" + args.units + "\")\" -o " + outLoc

print subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()
