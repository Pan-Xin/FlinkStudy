
frequency_dict = dict()

word_file = open('words.txt', 'r')

for line in word_file:
	line = line.strip('\n')
	cur = line.split(',')
	if frequency_dict.has_key(cur[0]):
		frequency_dict[cur[0]] = frequency_dict[cur[0]] + int(cur[1])
	else:
		frequency_dict[cur[0]] = int(cur[1])

word_file.close()

frequency_dict = sorted(frequency_dict.items(), key = lambda d: d[1], reverse = True)

for i in range(10):
	print(frequency_dict[i])
