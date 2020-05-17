import os
import glob
import psycopg2
import pandas as pd
import datetime as dt
import json
from sql_queries import *


def process_song_file(cur, filepath):
    """Processes single song file from data/song_data/.
    
       1. Opens song file. 
       2. Inserts 'song_id', 'title', 'artist_id', 'year', 'duration' as one record into the songs table.
       3. Inserts 'artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude' as one record into the artists table.
    """

    # open song file
    songs_df = pd.read_json(filepath, lines=True)

    # insert song record
    songs = songs_df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = songs.values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artists = songs_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artists.values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """Processes single log file (including multiple records per day) from data/log_data/.
        
       1. Opens log file. 
       2. Filters only the logs from the NextSong page.
       3. Converts column 'ts' in milliseconds to timestamps.
       4. Parses timestamps to 'hour', 'day', 'week', 'month', 'year', 'weekday'.
       5. Inserts all time data to time table.
       6. Inserts 'userId', 'firstName', 'lastName', 'gender', 'level' as one record into the users table.
       7. Creates a fact table called 'songplays' 
       8. Inserts 'start_time', 'user_id', 'level', 'song_id' (if exists), 'artist_id' (if exists), 'session_id', 'location', 'user_agent' as one record into the songplays table.
    """

    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_df = log_df.loc[log_df.page == "NextSong"]

    # convert timestamp (ts) column to datetime object
    t = log_df.ts.values
    t = t.reshape(-1, 1)

    time_data = []

    for i in range(len(t)):
        try:
            timestamp = ()
            start_time = dt.datetime.fromtimestamp(t[i][0] / 1000)

            # parse datetime object 
            start_time2 = dt.datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S.%f')

            # append start_time, hour, day, week, month, year, weekday as one element in time_data
            timestamp = (start_time, start_time2.strftime('%H'), start_time2.strftime('%d'),
                         start_time2.strftime('%U'), start_time2.strftime('%m'),
                         start_time2.strftime('%Y'), start_time2.strftime('%u'))
            time_data.append(timestamp)
        except KeyError:
            pass

    # create time dataframe and add time_data
    column_labels = ('start_time', 'hour', 'day',
                     'week', 'month', 'year', 'weekday')

    time_rows = []
    for time in time_data:
        time_rows.append(dict(zip(column_labels, time)))
        
    time_df = pd.DataFrame(time_rows, columns=column_labels)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_df = user_df.dropna(how='all')

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in log_df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, [row.song, row.artist, row.length])
        results = cur.fetchone()

        if results:
            songId, artistId = results
        else:
            songId, artistId = None, None

        # insert songplay record
        timestamp = dt.datetime.fromtimestamp(row.ts / 1000)
        songplay_data = (timestamp, row.userId, row.level, songId, artistId,
                         row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """Processes all files found in filepath using function func."""

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.set_session(autocommit=True)

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
