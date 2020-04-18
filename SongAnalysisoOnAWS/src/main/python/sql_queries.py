import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists user"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" create table if not exists staging_events (
artist        varchar,
auth          varchar not null,
firstname     varchar,
gender        varchar,
iteminsession int not null,
lastname      varchar,
length        numeric,
level         varchar not null, 
location      varchar,
method        varchar not null,
page          varchar not null,
registeration numeric,
sessionid     int not null,
song          varchar,
status        int not null,
timestmp      int not null,
useragent     varchar,
userid        int not null
)
""")

staging_songs_table_create = ("""create table if not exists staging_songs (
num_songs        Int Not Null,
artist_id        varchar Not Null,
artist_latitude  varchar, 
artist_longitude varchar,
artist_location  varchar,
artist_name      varchar Not Null,
song_id          varchar Not Null,
title            varchar Not Null,
duration         numeric Not Null,
year             Int Not Null
)
""")

songplay_table_create = ("""create table if not exists songplay(
songplay_id,start_time,user_id   int identity (0,1) primary key, 
start_time    timestamp not null, 
user_id       int not null, 
level         varchar not null, 
song_id       varchar, 
artist_id     varchar, 
session_id    int not null, 
location      varchar, 
user_agent    varchar not null
)
""")

user_table_create = ("""create table if not exists user(
user_id       int primary key, 
first_name    varchar not null, 
last_name     varchar not null, 
gender        varchar not null, 
level         varchar not null
)
""")

song_table_create = ("""create table if not exists song(
song_id       varchar primary key, 
title         varchar not null, 
artist_id     varchar not null, 
year          int not null, 
duration      numeric not null
)
""")

artist_table_create = ("""create table if not exists artist(
artist_id     varchar primary key, 
name          varchar not null, 
location      varchar, 
lattitude     numeric, 
longitude     numeric
)
""")

time_table_create = ("""create table if not exists time(
start_time    timestamp primary key, 
hour          int not null, 
day           int not null, 
week          int not null, 
month         int not null, 
year          int not null, 
weekday       int not null
)
""")

# STAGING TABLES

staging_events_copy = (""" 
copy staging_events from '{}'
iam_role '{}'
format as json {}
""").format(config.get ("S3","LOG_DATA"),config.get ("IAM_ROLE","ARN"), config.get ("S3","LOG_JSONPATH"))

staging_songs_copy = ("""
copy staging_songs from '{}'
iam_role '{}'
json 'auto'
""").format(config.get ("S3","SONG_DATA"),config.get ("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = (""" insert into songplay (songplay_id,start_time,user_id,level song_id,artist_id,location,user_agent)
select timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
  e.userId as user_id,
  e.level,
  s.song_id,
  s.artist_id,
  e.sessionId as session_id,
  e.location,
  e.userAgent as user_agent
from events_staging e
left join songs_staging s 
  on e.song = s.title and e.artist = s.artist_name
where e.page = 'NextSong'
""")

user_table_insert = (""" insert into user (user_id,first_name,last_name,gender,level)
Select usr.userid,
  usr.firstname,
  usr.lastname,
  usr.gender,
  usr.level
from (
    select row_number() over (partition by timestmp,userid order by timestmp desc) as rn,
      userid,
      firstname,
      lastname,
      gender,
      level
    from staging_events
    where page = 'NextSong'
     ) usr
where usr.rn=1
""")

song_table_insert = (""" insert into song (song_id,title,artist_id,year,duration)
select distinct song_id,
  title,
  artist_id,
  year,
  duration
from staging_songs
""")

artist_table_insert = ("""insert into artist(artist_id,name,location,latitude,longitude)
select distinct artist_id,
  artist_name,
  artist_location,
  artist_latitude,
  artist_longitude
from staging_songs
""")

time_table_insert = ("""insert into time (start_time, hour,day, week,month,year,weekday)
Select ti.timestmp,
  extract(hour from ti.timestmp ),
  extract(day from ti.timestmp ),
  extract(week from ti.timestmp ),
  extract(month from ti.timestmp ),
  extract(year from ti.timestmp ),
  extract(weekday from ti.timestmp )
from 
 (select distinct  timestamp 'epoch' + timestmp * interval '1 second' as timestmp
  from staging_events
  where page = 'NextSong') ti
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
