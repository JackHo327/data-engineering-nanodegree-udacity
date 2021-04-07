import os
import glob
import psycopg2
import numpy as np
import pandas as pd
from sql_queries import *

SONG_DATA_TYPES = {"num_songs": int, "artist_id": str, "artist_latitude": np.float64, "artist_longitude": np.float64, 
                   "artist_location": str, "artist_name": str, "song_id": str, "title": str, "duration": np.float64, "year": int}

LOG_DATA_TYPES = {"artist": str, "auth": str, "firstName": str, "gender": str, "itemInSession": int, "lastName": str, 
                  "length": np.float64, "level": str, "location": str,"method": str, "page": str, "registration": np.float64, 
                  "sessionId": int, "song": str, "status": int, "userAgent": str, "userId": str}

def process_song_file(cur, filepath):
    """Function that processes the song files and inserts records into songs and artists tables, respectively.
    """
    # open song file
    df = pd.read_json(filepath, dtype=SONG_DATA_TYPES, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].drop_duplicates()
    for vec in song_data.values:
        cur.execute(song_table_insert, vec)
    
    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].drop_duplicates()
    artist_data = artist_data.rename(columns={'artist_name':'name', 'artist_location':'location', 'artist_latitude':'latitude', 'artist_longitude':'longitude'})

    for vec in artist_data.values:
        # cur.execute(artist_table_insert, np.concatenate((vec, vec[2::]), axis=None))
        cur.execute(artist_table_insert, vec)

def process_log_file(cur, filepath):
    """Function that processes the log/event files and inserts records into time, users and songplays tables, respectively.
    """
    # open log file
    df = pd.read_json(filepath, dtype=LOG_DATA_TYPES, convert_dates=['ts'], lines=True)

    # filter by NextSong action
    df = df.loc[df['page'] == "NextSong"].drop_duplicates()
    
    # convert timestamp column to datetime
    # has been done when using read_json to set convert_dates=['ts']
    t = df.copy()
    
    # insert time data records
    time_data = zip(t.ts, t.ts.dt.hour, t.ts.dt.day, t.ts.dt.weekofyear, t.ts.dt.month, t.ts.dt.year, t.ts.dt.weekday_name)
    column_labels = ("start_time", "hour", "day", "week", "month", "year", "weekday")
    time_df = pd.DataFrame(data=list(time_data), columns=column_labels).drop_duplicates()

    # instead of using df.iterrows(), df.values is way much faster
    for vec in time_df.values:
        cur.execute(time_table_insert, vec)

    # load user table
    df = df.sort_values(['ts'])
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()
    user_df = user_df.rename(columns={'userId':'user_id', 'firstName':'first_name', 'lastName':'last_name'})

    # insert user records
    for vec in user_df.values:
        cur.execute(user_table_insert, np.concatenate((vec), axis=None))
    
    # data prep for songplay table - in order to use df.values
    df_column_list = df.columns.tolist()
    ts_index = df_column_list.index('ts')
    userId_index = df_column_list.index('userId')
    level_index = df_column_list.index('level')
    sessionId_index = df_column_list.index('sessionId')
    location_index = df_column_list.index('location')
    userAgent_index = df_column_list.index('userAgent')
    song_index = df_column_list.index('song')
    artist_index = df_column_list.index('artist')
    length_index = df_column_list.index('length')
    
    # insert songplay records
    for vec in df.values:
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (vec[song_index], vec[artist_index], vec[length_index],))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        # since these are just the sample files, so I do not want to apply too strict checks such as only for valid songid and artistid could be loaded into the db,
        # which means, under the current process, even for None songid and artistid values, these records could still be loaded into the db.
        songplay_data = (vec[ts_index], vec[userId_index], vec[level_index], songid, artistid, vec[sessionId_index], vec[location_index], vec[userAgent_index],)
        cur.execute(songplay_table_insert, songplay_data)



def process_data(cur, conn, filepath, func):
    """Function that collect all file paths and call the relevant data prep functions.
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
    
    # sort the data files 
    # - for log files, sort it based on the dates - so if there is a conflict when insert records into db, it alwas keep the latest value to represents users
    # - for song files, sort it based on its Track id
    all_files.sort()
    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed:{}'.format(i, num_files, datafile))


def main():
    """The main function of this ETL project, it will execute relevant functions to process the data and load data into relevant tables.
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()