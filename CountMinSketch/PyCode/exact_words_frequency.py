# this python file is used to compute the exact frequency of words
# the data has been pre-processed 

import math

# read the data file
word_file = open('words.txt', 'r')

# compute the frequency
frequency_dict = dict()
count = 0
for line in word_file:
	line = line.strip('\n')
	cur = line.split(',')
	if frequency_dict.has_key(cur[0]):
		frequency_dict[cur[0]] = frequency_dict[cur[0]] + int(cur[1])
	else:
		count += 1
		frequency_dict[cur[0]] = int(cur[1])

# close the data file
word_file.close()

# sort the dict in descending order
frequency_dict = sorted(frequency_dict.items(), key = lambda d: d[1], reverse = True)

# print the top k value
top_k = int(math.ceil(0.001 * count))
for i in range(top_k):
	print(frequency_dict[i])
