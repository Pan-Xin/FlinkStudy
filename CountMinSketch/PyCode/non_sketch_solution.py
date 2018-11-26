# this implements the non-sketch heavy hitters solution
# the data has been pre-processed 

import math

# read data file
word_file = open('words.txt', 'r')

K = 18

k_bins = dict()

# scan the data stream
for line in word_file:
	line = line.strip('\n')
	cur = line.split(',')
	item = cur[0]
	num = int(cur[1])
	# the misra_gries algorithm
	if item in k_bins:
		k_bins[item] += 1
	elif len(k_bins) < K - 1:
		k_bins[item] = 1
	else:
		for i in list(k_bins):
			k_bins[i] -= 1
			if k_bins[i] == 0:
				k_bins.pop(i)

# close the file
word_file.close()

# print the results
print(k_bins)


