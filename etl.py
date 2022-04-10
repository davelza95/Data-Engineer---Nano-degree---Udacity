import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    This function is responsible to insert into the 'song',
    and 'artist' tables the log file's information. 

    Args:
        cur (object): the cursor object.
        filepath (str): log data or song data file path
        
    Returns:
        None
    """        
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert song record
    song_data = df[["song_id",'title','artist_id','year','duration']].fillna('None').values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id','artist_name','artist_location','artist_latitude','artist_longitude']].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    This function is responsible to insert into the 'data',
    'user' and 'songplay' tables the log file's information.


    Args:
        cur (object): the cursor object.
        filepath (str): log data or song data file path.

    Returns:
        None
    """    
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df.loc[df.page == 'NextSong'].copy()

    # function created for convert in a list all the time information
    
    def time_data_list(x):
        return [x,x.hour,x.day,x.month,x.year,x.weekday()]
    
    # convert timestamp column to datetime
    
    t = pd.to_datetime(df.ts).apply(time_data_list)
    
    # insert time data records
    df['time_data'] = t
    time_data = t
    column_labels = ['timestamp', 'hour', 'day', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(df.time_data.values.tolist(),columns= column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.time_data[0],row.userId,row.level,songid,artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    This function is responsible for listing the files in a directory,
    and then executing the ingest process for each file according to the function
    that performs the transformation to save it to the database.

    Args:
        cur (object): the cursor object.
        conn (object): connection to the database.
        filepath (str): log data or song data file path.
        func (funct): fnction that transform the data and inserts in into a database.
    """    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    This function execute the all ETL process. 
    """    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()