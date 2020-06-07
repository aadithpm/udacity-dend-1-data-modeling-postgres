import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Reads data from a song file, runs any transformations we have defined and loads them into the database.
    
    keywords:
    cur - cursor to our PostgreSQL DB
    filepath - path to song data JSON
    
    returns:
    None
    """
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    song_data = list(song_data)
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values
    artist_data = [None if str(value) == 'nan' else value for value in list(artist_data)]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Reads data from a log file, runs any transformations we have defined and loads them into the database.
    
    keywords:
    cur - cursor to our PostgreSQL DB
    filepath - path to line separated log JSON
    
    returns:
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # Get timestamp, hour, day, week, month, year, weekday
    time_data = [
    df['ts'],
    t.dt.hour,
    t.dt.day,
    t.dt.week,
    t.dt.month,
    t.dt.year,
    t.dt.weekday
    ]
    # Get only values and make it a list
    time_data = [list(column.values) for column in time_data]

    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    time_df = pd.DataFrame(time_data).transpose()
    time_df.columns = column_labels 

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
            songid, artistid = 'None', 'None'

        # insert songplay record
        songplay_data = songplay_data = [
        row['ts'],
        row['userId'],
        row['level'],
        songid,
        artistid,
        row['sessionId'],
        row['location'],
        row['userAgent']
        ]

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Searches a path for any data files and runs the specified transformation function on all files in that path
    
    keywords:
    cur - cursor to our PostgreSQL DB
    conn - session for our PostgreSQL DB
    filepath - filepath to look for and list data files
    func - extraction and transformation function to use
    
    returns:
    None
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
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()