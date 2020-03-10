import os
import configparser as cp
import pandas as pd
import json
import psycopg2
from SongAnalysis.src.main.python.sql_queries import *


def process_data(cur, conn, filepath, func):
    for root, dirname, filename in os.walk(filepath):
        for files in filename:
            if '.json' in files:
                func(cur, conn, os.path.join(root,files))


def read_song_data(cur, conn, filepath):
    try:
        with open (filepath) as myfile:
            myfiledata= myfile.read()
            jsonDict = json.loads(myfiledata)
            df = pd.DataFrame([jsonDict])
            #print (df)
    except Exception as e:
        print (e)

    song_data = [df['song_id'].values,df['title'].values,df['artist_id'].values,df['year'].values,df['duration'].values]
    cur.execute(songs_insert,song_data)

    artist_data = [df['artist_id'].values, df['name'].values, df['location'].values, df['latitude'].values,df['longitude'].values]
    cur.execute(artists_insert,artist_data)
    conn.commit()

def main():
    env = 'dev'
    conf = cp.RawConfigParser()
    conf.read('../../../resources/application.properties')
    log_files_dir = conf.get(env, 'LogData')
    song_files_dir = conf.get(env, 'SongData')

    hostname = conf.get(env, 'DBhost')
    dbname = conf.get(env, 'DBName')
    username = conf.get(env, 'UserName')
    password = conf.get(env, 'Password')
    connect_str = 'host=' + hostname + ' dbname=sparkifydb user=' + username + ' password=' + password

    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()

    pd.set_option('display.width', 7000)
    pd.set_option('display.max_columns', 10)

    # process_data(log_files_dir)
    try:
        process_data(cur, conn, song_files_dir, read_song_data)
    except Exception as e:
        print (e)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()