#!/usr/bin/python

import sys

lastWord = None
sum = 0
for line in sys.stdin:
    word, count = line.split("\t", 1)
    if word != lastWord:
        if lastWord != None:
            print lastWord + "\t" + str(sum)
        lastWord = word
        sum = 0
    sum += int(count)

if lastWord != None:
    print lastWord + "\t" + str(sum)
