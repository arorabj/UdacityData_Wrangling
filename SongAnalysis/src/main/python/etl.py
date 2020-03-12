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
    except Exception as e:
        print (e)

    song_data = [df.values[0][6],df.values[0][7],df.values[0][1],df.values[0][9],df.values[0][8]]
    #print(song_data[0])
    cur.execute(songs_insert,song_data)

    artist_data = [df.values[0][1],df.values[0][5],df.values[0][4],df.values[0][2],df.values[0][3]]
    #artist_data.info()
    cur.execute(artists_insert,artist_data)
    conn.commit()

def read_log_data (cur, conn, filepath):
    try:
        df = pd.read_json(filepath,lines = True)
        df = df[df['page'] == 'NextSong']
        t = pd.to_datetime(df['ts'],unit='ms')
        time_data = pd.concat([t,t.dt.hour,t.dt.day,t.dt.week,t.dt.month,t.dt.year,t.dt.weekday],axis=1)
        time_df = pd.DataFrame(time_data.values,columns=('start_time','hour','day','week','month','year','weekday'))

        for i,row in time_df.iterrows():
            cur.execute(time_insert,list(row))

        conn.commit()

        user_data = df[['userId','firstName','lastName','gender','level']]

        for i,row in user_data.iterrows():
            cur.execute(users_insert,row)
        conn.commit()

        for i, row in df.iterrows():
            cur.execute(songs_select,(row.song,row.artist,row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None

            cur.execute(songplays_insert, (pd.to_datetime(row.ts,unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent))
        conn.commit()
    except Exception as e:
        print(e)

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
    pd.set_option('display.max_columns', 100)

    try:
        process_data(cur, conn, song_files_dir, read_song_data)
        process_data(cur, conn, log_files_dir, read_log_data)
    except Exception as e:
        print (e)
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()