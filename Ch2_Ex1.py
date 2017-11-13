import os
import multiprocessing as mp
import sqlite3
from datetime import datetime
import string
import getsubredditname


# Function to find the unique word vocabulary for subreddit
def querying(Subreddit):
    #Connect to the reddit database. 
    conn = sqlite3.connect("reddit.db")
    #Cursor to the reddit database.
    cursor = conn.cursor()

    #Query to find all comments for the corresponding subreddit_id  
    query =  """
                Select body
                From Comments      
                Where subreddit_id = ?
            """
    # Fetch all of the data from the query.
    cursor.execute(query,Subreddit)     

    # To clean the comment of punctuation letters, eg & % ´ and so on and replace it with white space
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation))
    # Create set to hold for all of the unique words
    wordSet = set()
    # Go through all of the comments
    for Comment in cursor.fetchall():
        # First clean the comment and set all upper characters to lower
        Comment = Comment[0].translate(translator).lower()
        # Update the unique word set with new words
        wordSet.update(tuple(word for word in Comment.split() if word))

    # Return the subbredit_id and corresponding number of unique words.
    return (Subreddit[0],len((wordSet)))
          


 
if __name__ == '__main__':
    #Start the time.
    start_time = datetime.now()
    # Connect to the reddit database
    conn = sqlite3.connect("reddit.db")
    #Cursor to the reddit
    cursor = conn.cursor()
    # Query to find all subreddit_id of the database
    query2 = """SELECT id
            From subreddits
            """
    # Execute the query
    cursor.execute(query2)
    # Create number of processors for the multiprocesses
    PROCESS = mp.cpu_count() # Utilize all of the process power available 
    p = mp.Pool(PROCESS)
    # Fetchall the data and utilized the number of active processors
    # to find the number of unique words vocabulary for each subreddit
    CHUNKSIZE = 10
    results = p.map(querying, cursor.fetchall(),chunksize=CHUNKSIZE)
    # Close the multiprocess
    p.close()
    p.join()
    # Print out the table of top ten unique word vocabularies and find the match subreddit names
    getsubredditname.sort_and_find(results,1,"reddit.db")
    # Print the Total Time
    print('Total Time: {0}'.format(datetime.now()-start_time))