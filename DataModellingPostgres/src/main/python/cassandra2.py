import cassandra
from cassandra.cluster import Cluster

conn =  Cluster(['127.0.0.1'])
session = conn.connect()

try:
    session.execute("CREATE KEYSPACE IF NOT EXISTS udacity WITH REPLICATION ={'class': 'SimpleStrategy','replication_factor':1}")
except Exception as e:
    print (e)


try:
    session.set_keyspace('udacity')
except Exception as e:
    print (e)

try:
    session.execute('CREATE TABLE IF NOT EXISTS MUSIC_LIBRARY (YEAR INT, ARTIST_NAME VARCHAR, ALBUM_NAME VARCHAR, PRIMARY KEY (YEAR, ARTIST_NAME))')
except Exception as e:
    print (e)



try:
    session.execute('INSERT INTO MUSIC_LIBRARY (YEAR, ARTIST_NAME, ALBUM_NAME ) VALUES (%s,%s,%s)', (1970, "The Beatles", "Let it Be"))
except Exception as e:
    print (e)


try:
    rows=session.execute('select * from MUSIC_LIBRARY where YEAR= 1970')
except Exception as e:
    print (e)

for i in rows:
    print( i.year, i.artist_name, i.album_name)

session.shutdown()
conn.shutdown()
