from io import open
import time


index = {}


def print_usage():
		print '\nUsage:'
		print 'index <version> : selects the version of the index to use'
		print '\tversion can be either hadoop or spark'
		print 'search <keywords> : searches for the given keywords'
		print '\t keywords can be a space-delimited list of words'
		print 'help : displays this helpful usage information'
		print 'quit : exits the program.\n'


def loadInvIndex(version):
	global index
	index.clear()
	path = "/Users/risinger/Documents/edu/CloudComputing/Project/"
	time_path = "/Users/risinger/Documents/edu/CloudComputing/Project/"
	if version == 'spark':
		path += 'output/spark_output.txt/part-00000'
		time_path += 'output/spark_time.txt'
	else:
		path += 'output/hadoop/part-r-00000'
		time_path += 'output/hadoop_time.txt'

	with open(path, encoding='utf-8') as f:
		for line in f:
			commaIndex = line.index('\t')
			word = line[:commaIndex]
			bookList = line[commaIndex+1:].strip().split('\t')
			newList = []
			for pair in bookList:
				split = pair.strip('(').strip(')').split(',')
				newList.append( (split[0], int(split[1])) )
			index[word] = newList

	with open(time_path, encoding='utf-8') as f:
		time = f.readline()
		print 'Time to index: {0}'.format(time)


def search(keywords):
	global index
	start_time = time.time()

	results = {}
	totals = {}

	for keyword in keywords:
		if keyword in index:
			for book, count in index[keyword]:
				if book in results:
					results[book].append( (keyword, count) )
					totals[book] += count
				else:
					results[book] = [(keyword, count)]
					totals[book] = count

	ordered_results = []
	for book in totals.keys():
		new_result = (book, results[book])
		if len(ordered_results) == 0:
			ordered_results.append(new_result)
			continue
		added = False
		for i, o_r in enumerate(ordered_results):
			if totals[book] >= totals[o_r[0]]:
				ordered_results.insert(i, new_result)
				added = True
				break
		if added == False:
			ordered_results.append(new_result)

	for (book, word_count_list) in ordered_results:
		print '\n{0}:\t{1} total occurances'.format(book, totals[book])
		for word, count in word_count_list:
			print '\t{0}: {1} occurances'.format(word, count)

	end_time = time.time()
	print '\nSearch took {0} seconds\n'.format(end_time - start_time)


while True:
	command = raw_input('Please enter a command:\n')
	command = command.strip().split(" ")
	if command[0] == 'index':
		if command[1] != "hadoop" and command[1] != "spark":
			print 'Options for index are either hadoop or spark.'
		else:
			loadInvIndex(command[1])
	elif command[0] == 'search':
		if len(index) == 0:
			print 'Please select the index first.'
		else:
			search(command[1:])
	elif command[0] == 'help':
		print_usage()
	elif command[0] == 'quit':
		break;
	else:
		print 'Command not recognized.'
		print_usage()