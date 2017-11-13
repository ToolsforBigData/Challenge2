from __future__ import division
import sqlite3
from datetime import datetime
import multiprocessing as mp
import getsubredditname

def get_subreddits():
    #Database path
    DB_PATH = "reddit.db"
    #Establish connection and create cursor
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    #Select all subreddit ids in descending order 
    #regarding amount of comments
    query = '''
       SELECT subreddit_id
           FROM comments
           GROUP BY subreddit_id
           ORDER BY count(ROWID) DESC
     '''
    #Exectue query and return all subreddit ids
    cursor.execute(query)
    return cursor.fetchall()

def querying(Subreddit):

    #Query to get top comments (all that begin with t3)
    query_get_top_comments = '''
    SELECT id
        FROM comments
        WHERE subreddit_id = ?
        AND parent_id LIKE 't3%'
    '''

    #Recursive function to fetch all branches and find the deepest branch.
    # It is combined on comments.id and parents.id, It returns the max (depth)
    # value for each top (t3) comment 
    query_get_max_depth ='''
        WITH recursive
            tree(name,level) AS (
                VALUES(?,0)
                UNION ALL
                SELECT comments.id, tree.level +1
                    FROM comments JOIN tree ON comments.parent_id = tree.name
            )
            SELECT MAX(level) FROM tree;
    '''

    #Database path
    DB_PATH = "reddit.db"
    #Establish connection and create cursor
    conn = sqlite3.connect(DB_PATH,check_same_thread = False)
    cursor = conn.cursor()

    #Initialize counter and depth value for the subreddit in order to compute average
    depth = 0
    counter = 0

    # Loop over each top comment (t3) and run the recursive query
    for top_comment_id in conn.execute(query_get_top_comments,Subreddit):

        #Get the max value for the t3 and add to value and count.
        cursor.execute(query_get_max_depth,top_comment_id)

        data = cursor.fetchall()
        counter = counter + 1
        depth = depth + data[0][0]
        
    #Lets not divide by zero and create WW2    
    if counter == 0:
        return [Subreddit,0]

    return (Subreddit[0],depth/counter)
          
 
if __name__ == '__main__':
    #Start timer
    start_time = datetime.now()
    #Get all subreddits to feed into multiprocessing
    all_subreddits = get_subreddits()

    PROCESS = mp.cpu_count() # Utilize all of the process power available 
    p = mp.Pool(PROCESS) #Start a multiprocess with 4 processes.

    CHUNKSIZE = 1 # Best for this problem to have chunks of one
    results = p.map(querying, all_subreddits,chunksize=CHUNKSIZE)

    #Turn off multiprocesses
    p.close()
    p.join()

    # Print out the table of top ten unique word vocabularies and find the match subreddit names
    getsubredditname.sort_and_find(results,1,"reddit.db")
    # Print the Total Time
    print('Total Time: {0}'.format(datetime.now()-start_time))
