#/usr/bin/env python

import time, sys

def process(src, dst):
	fout = open(dst, "w")
	arr = []
	for line in open(src):
		line = line.strip()
		lst = line.split("\t")
		if len(lst) < 2 or lst[1] == "-":	
			continue
		arr.append((lst[0], lst[1]))
	arr = sorted(arr, key=lambda x: int(x[1]))
	for elem in arr:
		fout.write(elem[0] + "\t" + elem[1] + "\n")
	fout.close()

def main():
	if len(sys.argv) < 3:
		print "Usage:", sys.argv[0], "<input-file> <output-file>"
		sys.exit(1)
	process(sys.argv[1], sys.argv[2])


if __name__ == "__main__":
	main()
