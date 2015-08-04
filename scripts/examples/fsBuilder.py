# Friction Surface Builder script
import sys, os, subprocess, datetime, argparse, random

now = datetime.datetime.now()
nowf = now.strftime("%Y%m%d-%s")

# User input
parser = argparse.ArgumentParser()
parser.add_argument("-e", "--equation", help="Friction surface equation to utilize. Defaults to tobler.", choices=['tobler','pingel'], action="store", default="tobler")
parser.add_argument("-m", "--maxspeed", help="Maximum walking or vehicle speed in kilometers per hour. Defaults to 6kph.", action="store", default=6)
parser.add_argument("-o", "--output", help="S3 or HDFS location for results.", action="store", default="s3://mrgeo/images/fsout-" + nowf)
parser.add_argument("-s", "--scratch", help="Run friction surface recipe from scratch. Will take much longer.", action="store_true")
args = parser.parse_args()

# Set up S3 links
elevationRaster = 's3://mrgeo/images/aster-30m'
slopeRaster = 's3://mrgeo/images/aster-30m-slope-deg'
impedanceRaster = 's3://mrgeo/images/gc-30m-impedance'
roadwayVector = 's3://mrgeo-source/roads.tsv'
roadwayRaster = 's3://mrgeo/images/roadways-kph'
waterRaster = 's3://mrgeo/images/srtm-waterbodies'
waterVector = 's3://mrgeo-source/srtm-waterbodies'

# Modify equation based on user input
if args.equation == 'tobler':
	mult = "-3.5"
	ab = "abs(tan(slope) + 0.05)"
	maxslope = "60.0"
else:
	mult = "-8.3"
	ab = "abs(tan(slope))"
	maxslope = "40.0"

# Get max speed to apply
ms = str(args.maxspeed)

# Run from scratch or from pre-processed inputs
if args.scratch:
	ma = "slope = slope([" + elevationRaster + "]); \
roads = RasterizeVector([" + roadwayVector + "],\"MAX\",\"12z\",\"b\"; \
imp = con(abs([s3://mrgeo/images/GlobCover30m] - 11) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 14) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 20) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 30) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 40) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 50) < 0.1, 0.2, \
abs([s3://mrgeo/images/GlobCover30m] - 60) < 0.1, 0.5, \
abs([s3://mrgeo/images/GlobCover30m] - 70) < 0.1, 0.2, \
abs([s3://mrgeo/images/GlobCover30m] - 90) < 0.1, 0.3, \
abs([s3://mrgeo/images/GlobCover30m] - 100) < 0.1, 0.4, \
abs([s3://mrgeo/images/GlobCover30m] - 110) < 0.1, 0.2, \
abs([s3://mrgeo/images/GlobCover30m] - 120) < 0.1, 0.7, \
abs([s3://mrgeo/images/GlobCover30m] - 130) < 0.1, 0.2, \
abs([s3://mrgeo/images/GlobCover30m] - 140) < 0.1, 0.7, \
abs([s3://mrgeo/images/GlobCover30m] - 150) < 0.1, 0.8, \
abs([s3://mrgeo/images/GlobCover30m] - 160) < 0.1, 0.2, \
abs([s3://mrgeo/images/GlobCover30m] - 170) < 0.1, 0.1, \
abs([s3://mrgeo/images/GlobCover30m] - 180) < 0.1, 0.1, \
abs([s3://mrgeo/images/GlobCover30m] - 190) < 0.1, 0.6, \
abs([s3://mrgeo/images/GlobCover30m] - 200) < 0.1, 1.0, \
abs([s3://mrgeo/images/GlobCover30m] - 210) < 0.1, 0.0, \
abs([s3://mrgeo/images/GlobCover30m] - 220) < 0.1, 0.5, \
abs([s3://mrgeo/images/GlobCover30m] - 230) < 0.1, 1.0, 0.6); \
water = RasterizeVector([" + waterVector + "],\"MASK\",\"12z\"); \
fs = 3.6 / (" + ms + " * pow(2.718281828, " + mult + " * " + ab + ")); \
result = con(slope > " + maxslope + ", NaN, water = 0, NaN, roads > 0, roads, (3.6 / (fs * imp)));"
	cmd = "mrgeo mapalgebra -e " + "\"" + ma + "\"" + " -o " + args.output

else:
	ma = "slope = [" + slopeRaster + "];fs = 3.6 / (" + ms + " * pow(2.718281828, " + mult + " * " + ab + ")); \
result = con([" + slopeRaster + "] > " + maxslope + ", NaN, [" + waterRaster + "] = 0, NaN, [" + roadwayRaster + "] > 0, [" + roadwayRaster + "], (3.6 / (fs * [" + impedanceRaster + "])))"
	cmd = "mrgeo mapalgebra -e " + "\"" + ma + "\"" + " -o " + args.output

print subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE).stdout.read()

