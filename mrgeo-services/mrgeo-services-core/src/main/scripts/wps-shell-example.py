#!/usr/bin/python

import sys
import time

# read all the input values into a dictionary for easy access.
args = {}
for v in sys.argv[1:]:
	s = v.split("=")
	args[s[0]] = s[1]

# put an artificial pause to simulate an expensive operation
for i in range(1,10):
	print "progress:" + str(i * 10)
	time.sleep(1)

# write out the results.
print "summary:" + str(args)	
print "output:" + args['output']
