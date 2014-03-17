#!/usr/bin/env python
#coding=utf-8
# description: 
# author: zxd
# date: 2013-10-22

import sys, os, time
import random
import commands

GlobalDict = set()

def loadDict(dictFile):
	global GlobalDict
	for line in open(dictFile):
		line = line.strip()
		if line == "":	continue
		lst = line.split("\t")
		GlobalDict.add(lst[0])

def process(srcFile, dstFile):
	global GlobalDict
	count = 0
	numSampled = 0
	fout1 = open(dstFile, "w")
	for line in open(srcFile):
		if line.strip() == "":	continue
		pos = line.find("\t")
		if pos == -1:
			key = line.strip()
		else:
			key = line[0:pos]
		if key in GlobalDict:
			fout1.write(line)
			numSampled += 1	
		count += 1
		if count % 10000 == 0:
			print "Having processed", count, "lines!"
	fout1.close()
	print "Having processed", count, "lines in total,", numSampled, "sampled."

def main():
	if len(sys.argv) < 4:
		print "Usage: " + sys.argv[0] + " <dict-file> <input-file> <output-file>"
		sys.exit(1)
	dictFile = sys.argv[1]
	srcFile = sys.argv[2]
	dstFile = sys.argv[3]
	loadDict(dictFile)
	process(srcFile, dstFile)

if __name__ == "__main__":
	main()

