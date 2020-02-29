import psycopg2 as p

try:
    conn = p.connect("host=127.0.0.1 dbname=testdb user=udacity password=test1234")
except p.Error as e:
    print("connection failed")
    print (e)

try:
    cur= conn.cursor()
except p.Error as e:
    print("initializing cursor failed")
    print(e)

conn.set_session(autocommit=True)

try:
    cur.execute('select * from songs')
except p.Error as e:
    print("Error while sending request for queriyng songs")
    print(e)

try:
    cur.execute('delete from songs')
except p.Error as e:
    print("Error while Deleting the data from songs")
    print(e)


try:
    cur.execute('create table if not exists songs (song_title varchar, artist_name varchar, year int, album_name varchar,single boolean)')
except p.Error as e:
    print("Error while creating table songs")
    print(e)


try:
    cur.execute("insert into songs (song_title, artist_name, year, album_name,single) \
                values (%s,%s,%s,%s,%s)",\
                ("Across The Universe", "The Beatles", "1970", "Let It Be","False"))
except p.Error as e:
    print("Error while Inserting 1st row in table songs")
    print(e)

try:
    cur.execute("insert into songs (song_title, artist_name, year, album_name,single) \
                values (%s,%s,%s,%s,%s)",\
                ("The Beatles", "Think For Yourself", "1965", "Rubber Soul","False"))
except p.Error as e:
    print("Error while Inserting 2nc row in table songs")
    print(e)


try:
    cur.execute("select * from songs")
except p.Error as e:
    print ("Select data from table songs failed")
    print (e)

row = cur.fetchone()
while row:
    print (row)
    row =cur.fetchone()

cur.close()
conn.close()