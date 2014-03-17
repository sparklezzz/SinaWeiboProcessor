#!/usr/bin/env python
#coding=utf-8
# description: 
# author: zxd
# date: 2013-10-22

import sys, os, time
import random
import commands

def process(srcFile, dstFile, ratio):
	'''cmd = "wc -l " + srcFile
	print cmd
	(status, output) = commands.getstatusoutput(cmd)
	totalLines = int(output.strip())'''
	count = 0
	numSampled = 0
	fout1 = open(dstFile, "w")
	for line in open(srcFile):
		if line.strip() == "":	continue
		ran = random.random()
		if ran <= ratio:
			fout1.write(line)
			numSampled += 1	
		count += 1
		if count % 10000 == 0:
			print "Having processed", count, "lines!"
	fout1.close()
	print "Having processed", count, "lines in total,", numSampled, "sampled."

def main():
	if len(sys.argv) < 4:
		print "Usage: " + sys.argv[0] + " <input-file> <output-file> <ratio>"
		sys.exit(1)
	srcFile = sys.argv[1]
	dstFile = sys.argv[2]
	ratio = float(sys.argv[3])
	process(srcFile, dstFile, ratio)

if __name__ == "__main__":
	main()

