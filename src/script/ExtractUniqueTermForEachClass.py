#!/usr/bin/env python
# Description: given several classes of terms, make every term belongs to only on classes, according to its highest freq. 

import sys, os, re

def process(inputDir, outputFile):
	labelNum = 33
	curLabel = 1
	TermLabelFreqDict = {}
	while curLabel <= labelNum:
		filePath = inputDir + "/topwords" + str(curLabel) + ".txt"
		print "Processing", filePath, "..."
		for line in open(filePath):
			lst = line.strip().split("\t")
			if len(lst) < 2:	
				continue
			if lst[0] not in TermLabelFreqDict:
				TermLabelFreqDict[lst[0]] = (curLabel, int(lst[1]))
			else:
				if TermLabelFreqDict[lst[0]][1] < int(lst[1]):	#choose label of higher freq
					TermLabelFreqDict[lst[0]] = (curLabel, int(lst[1]))
		curLabel += 1

	fout = open(outputFile, "w")
	for key in TermLabelFreqDict:
		fout.write("%s\t%d\t%d\n" % (key, TermLabelFreqDict[key][0], TermLabelFreqDict[key][1]))
	fout.close()

def main():
	if len(sys.argv) < 3:
		print "Usage", sys.argv[0], "<input-dir> <output-file>"
		sys.exit(1)
	process(sys.argv[1], sys.argv[2])

if __name__ == "__main__":
	main()
