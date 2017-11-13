import sqlite3
from datetime import datetime
from multiprocessing import Pool
import sys
import itertools


def getTopSubreddits(TOP):
	# The path for the database
	DB_PATH = "reddit.db"

	# Start the connection for sqlite3
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()

	# Query to find all the subreddit_ids with the total number of comments for that subreddit
	query = '''
			SELECT subreddit_id, count(ROWID)
			FROM comments
			GROUP BY subreddit_id
	'''
	# Execute the query
	cursor.execute(query)
	# The result from the query set at the variable data
	data =  cursor.fetchall()

	# Sort the data by the total number of subreddits in a reverse oder
	list_sorted = sorted(data, key=lambda x: x[1],reverse=True)
	# Sort out to take only the 'TOP' numbers of subreddits
	list_sort = [i[0] for i in list_sorted[:TOP]]
	# Sort out to take only the 'TOP' numbers of comments for the subreddits
	list_sort_extra = [i[1] for i in list_sorted[:TOP]]

	return list_sort,list_sort_extra


def funQuery(subreddit):
	# The path for the database
	DB_PATH = "reddit.db"
	# Start the connection for sqlite3
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()

	# Database that gets all the author_id for a specific subreddit_id
	query = '''
	    	SELECT author_id
	        FROM comments
	        WHERE subreddit_id = ?
	'''
	# Execute the query with the subreddit as an input
	cursor.execute(query,[subreddit])

	# Set the output of the query as data
	data = cursor.fetchall()
	return [subreddit,set(data)]


#-------------THE MAIN FUNCTION --------------#

if __name__ == '__main__':
	# Start the clock!
	t_start = datetime.now()

	# The inital guess of subreddits that was optimized
	initial_number = 121
	numbSub = initial_number

	while True:
		# Set the time
		t0 = datetime.now()
		# Get the subreddits with the most comments and the number of comments
		topSubs,topSubsVal = getTopSubreddits(numbSub)
		# Set the time
		t1 = datetime.now()

		# Start a multiprocess with 10 processes.
		p = Pool(10)
		# Run the querying function on 10 processes
		results = p.map(funQuery, topSubs)
		# Set the time
		t2 = datetime.now()

		# Initialize variable to get the pairs and the number of authors
		unqPairs = []
		for i in itertools.combinations(results,2):
			# The number of same authors for the two subreddits
			no_same_authors = len(set(i[0][1].intersection(i[1][1])))
			# Append the pairs and the number of authors they have in common
			unqPairs.append([i[0][0],i[1][0],no_same_authors])

		# Turn off multiprocesses
		p.close()
		p.join()

		# Set the time
		t3 = datetime.now()

		#  Print the time for different function
		print('Get Top Comment Subreddit time:{}'.format(t1-t0))
		print('Query timer: {}'.format(t2-t1))
		print('Set timer {}'.format(t3-t2))

		# Sort the unique pairs according to the number of common authors
		sort_list =sorted(unqPairs, key=lambda x: x[2],reverse=True)

		# Check if:
			# sort_list[9][-1]: the number of common authors for the 10th pair
			# is greater than
			# topSubsVal[-1]: the total number of comments for the last subreddit
		if sort_list[9][-1] > topSubsVal[-1]:
			break

		# Jump to the next value
		numbSub = numbSub + initial_number


	#-------------PRINT THE COOL RESULTS--------------#

	# The path for the database
	DB_PATH = "reddit.db"
	# Start the connection for sqlite3
	conn = sqlite3.connect(DB_PATH)
	cursor = conn.cursor()

	# Set the counter to zero and choose the top 10 pairs
	counter = 0
	Top_list = 9
	# Print the header of the table
	print('{}.  {:<20}, {:<20} and {:<20}, {:<20} with {:<10} '.format('#','Subreddit 1 ','Subreddit_id 1','Subreddit 2','Subreddit_id 2','Count'))
	for i in sort_list:
		counter = counter + 1

		# Query to get the names of the subreddits
		q = '''
			SELECT name
				FROM subreddits
				WHERE id = ?
		'''

		# Execute the query for the first subreddit in the pair and then the second
		cursor.execute(q,[i[0]])
		sub1 = cursor.fetchone()
		cursor.execute(q,[i[1]])
		sub2 = cursor.fetchone()

		# Print the rest of the table
		print('{}.  {:<20}, {:<20} and {:<20}, {:<20} with {:<10} '.format(counter,sub1[0],i[0],sub2[0],i[1],i[2]))

		# Break when the table has reached the number for the top list
		if counter > Top_list:
			break
	# Get the final time
	t_end = datetime.now()

	# Print the Total run time
	print('Total Time: {0}'.format(t_end-t_start))
