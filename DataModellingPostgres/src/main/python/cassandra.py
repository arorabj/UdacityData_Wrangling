import cassandra
from cassandra.cluster import Cluster


try:
    conn = Cluster(['127.0.0.1'])
    session = conn.connect()
except Exception as e:
    print("Error in Connection")
    print (e)


try:
    session.execute("CREATE KEYSPACE IF NOT EXISTS udacity WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor':1}")
except Exception as e:
    print("Error creating keyspace")
    print (e)

try:
    session.set_keyspace('udacity')
except Exception as e:
    print('Error setting up keyspace')
    print(e)

query = "CREATE TABLE IF NOT EXISTS songs "
query = query + "(song_title text, artist_name text, year int, album_name text, single boolean, PRIMARY KEY (year, artist_name ))"
try:
    session.execute (query)
except Exception as e:
    print('Error setting up keyspace')
    print(e)

query = "INSERT INTO songs (song_title, artist_name, year, album_name, single) "
query = query + "values (%s,%s,%s,%s,%s)"

