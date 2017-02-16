#!/usr/bin/python

import os
import subprocess
import sys

# read all the input values into a dictionary for easy access.
args = {}
for v in sys.argv[1:]:
    s = v.split("=")
    args[s[0]] = s[1]

hh = os.getenv('HADOOP_HOME')
mh = os.getenv('MRGEO_HOME')

result = subprocess.call([hh + "/bin/hadoop", "jar", hh + "/contrib/streaming/hadoop-0.20.1-streaming.jar",
                          "-input", args['input'],
                          "-output", args['output'],
                          "-mapper", mh + "/scripts/word-mapper.py " + ' '.join(args['validWords'].split(',')),
                          "-reducer", mh + "/scripts/word-reducer.py",
                          "-file", mh + "/scripts/word-mapper.py",
                          "-file", mh + "/scripts/word-reducer.py"])

if result != 0:
    sys.exit(result)

# write out the results.
print "summary:" + str(args)
print "output:" + args['output']
