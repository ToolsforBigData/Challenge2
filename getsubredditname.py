import sqlite3

def sort_and_find(arr,column_to_sort_by,database):

	#Establish connection
	conn = sqlite3.connect(database)
	cursor = conn.cursor()
	#Sort list to find top 10
	list_sorted =sorted(arr, key=lambda x: x[column_to_sort_by],reverse=True)


	counter = 0
	#Print header.
	print('{:<2}{:<12}{:<18}{:<10}'.format('##','Subreddit','Subreddit_ID','Value'))
	for i in list_sorted:
		#Count to print
		counter = counter + 1
		#Query to get the names of subreddits.
		q = '''
			SELECT name
				FROM subreddits
				WHERE id = ?
		'''
		#Fetch the data
		name_list = []
		cursor.execute(q,[i[0]])
		data = cursor.fetchall()
		name_list.append(data[0])
		#Print the data and results 
		if len(name_list) > 1:
			print('{:<2}{:<12}{:<18}{:<10.2f} '.format(counter,name_list[0][0],i[0],name_list[1][0],i[1]))
		else:
			print('{:<2}{:<12}{:<18}{:<10.2f} '.format(counter,name_list[0][0],i[0],i[1]))
		#Print only top 10
		if counter > 9:
			break
