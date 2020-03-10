import psycopg2
import configparser as cp
from SongAnalysis.src.main.python.sql_queries import drop_tables_list, create_tables_list

def main():
    conf = cp.RawConfigParser()
    env='dev'
    conf.read('../../../resources/application.properties')

    hostname = conf.get(env,'DBhost')
    dbname = conf.get(env,'DBName')
    username = conf.get(env,'UserName')
    password = conf.get(env,'Password')

    conn, cur = create_database(hostname,dbname,username,password)
    print (drop_tables_list)
    print (create_tables_list)
    drop_tables(conn, cur , drop_tables_list)
    create_tables(conn, cur, create_tables_list)

    cur.close()
    conn.close()

def create_database(hostname,dbname,username,password):
    connect_str = 'host=' + hostname + ' dbname=' + dbname + ' user=' + username + ' password=' + password
    #connect to testdb by default
    conn = psycopg2.connect(connect_str)
    cur = conn.cursor()
    conn.set_session (autocommit = True)

    #create actual sparkifydb database
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute ("CREATE DATABASE sparkifydb with encoding 'utf-8' TEMPLATE template0")

    cur.close()
    conn.close()

    # connect to acutal database
    connect_str = 'host=' + hostname +' dbname=sparkifydb user=' +username +' password='  +password
    conn = psycopg2.connect(connect_str)
    cur=conn.cursor()

    return conn, cur

def drop_tables(conn, cur, drop_tables_list):
    # create tables
    for i in drop_tables_list:
        try:
            # print  (i)
            cur.execute(i)
            conn.commit()
        except Exception as e:
            print(i)
            print (e)

def create_tables(conn, cur, create_tables_list):
    # create tables
    for i in create_tables_list:
        try:
            # print(i)
            cur.execute(i)
            conn.commit()
        except Exception as e:
            print(i)
            print (e)

if __name__ == "__main__":
    main()