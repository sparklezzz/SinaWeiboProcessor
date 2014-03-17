#!/usr/bin/env python
#coding=utf-8
# description: 
# author: zxd
# date: 2013-10-22

import sys, os, time

def process(srcFile, trainFile, testFile):
	count = 0
	fout1 = open(trainFile, "w")
	fout2 = open(testFile, "w")
	for line in open(srcFile):
		lst = line.strip().split("\t")
		if len(lst) == 0:	continue
		elif len(lst) == 1 or lst[1] == "-":
			fout2.write(line)
		else:
			fout1.write(line)
		count += 1
		if count % 10000 == 0:
			print "Having processed", count, "lines!"
	fout1.close()
	fout2.close()
	print "Having processed", count, "lines in total."

def main():
	if len(sys.argv) < 4:
		print "Usage: " + sys.argv[0] + " <input-file> <train-file> <test-file>"
		sys.exit(1)
	srcFile = sys.argv[1]
	trainFile = sys.argv[2]
	testFile = sys.argv[3] 
	process(srcFile, trainFile, testFile)

if __name__ == "__main__":
	main()

