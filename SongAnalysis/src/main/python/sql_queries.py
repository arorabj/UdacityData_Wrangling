# drop tables
songplays_drop="""DROP TABLE IF EXISTS songplays"""
users_drop="""DROP TABLE IF EXISTS users"""
artists_drop="""DROP TABLE IF EXISTS artists"""
songs_drop="""DROP TABLE IF EXISTS songs"""
time_drop="""DROP TABLE IF EXISTS time"""


# create tables
songplays_create="""CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial primary key,
start_time timestamp,
user_id int not null,
level varchar,
song_id varchar,
artist_id varchar,
session_id int,
location varchar not null
)"""

users_create="""CREATE TABLE IF NOT EXISTS users(
user_id int primary key, 
first_name varchar not null, 
last_name varchar, 
gender varchar not null, 
level varchar 
)"""

artists_create="""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar primary key, 
name varchar not null, 
location varchar, 
latitude float, 
longitude float
)"""

songs_create="""CREATE TABLE IF NOT EXISTS songs (
song_id varchar primary key, 
title varchar not null, 
artist_id varchar not null, 
year int not null, 
duration float
)"""

time_create="""CREATE TABLE IF NOT EXISTS time (
start_time timestamp primary key, 
hour int not null, 
day int not null, 
week int not null, 
month int not null, 
year int not null, 
weekday int not null
)"""

drop_tables_list=[songplays_drop,users_drop,artists_drop,songs_drop,time_drop]
create_tables_list=[songplays_create,users_create,artists_create,time_create,songs_create]

# insert
songplays_insert = ("""insert into songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent) values (%s,%s,%s,%s,%s,%s,%s,%s) on CONFLICT DO NOTHING
""")
users_insert = ("""insert into users (user_id,first_name,last_name,gender,level) values (%s,%s,%s,%s,%s) ON CONFLICT (user_id) DO UPDATE set level = EXCLUDED.level
""")
songs_insert = ("""insert into songs (song_id,title,artist_id,year,duration) values (%s,%s,%s,%s,%s) ON CONFLICT (song_id) DO NOTHING
""")
artists_insert = ("""insert into artists (artist_id,name,location,latitude,longitude) values (%s,%s,%s,%s,%s) ON CONFLICT (artist_id) DO NOTHING
""")
time_insert = ("""insert into time (start_time,hour,day,week,month,year,weekday) values (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (start_time) DO NOTHING
""")

# FIND SONGS
songs_select = ("""select song_id,  songs.artist_id from songs inner join artists on songs.artist_id=artists.artist_id where songs.title = %s and artists.name= %s and songs.duration = %s
""")
