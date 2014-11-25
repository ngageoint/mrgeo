#!/usr/bin/python

import sys

validWords = set(sys.argv[1:])

for line in sys.stdin:
	words = line.lower().split(' ')
	for w in words:
		w = w.strip()
		if w in validWords:
			print w + "\t1"
