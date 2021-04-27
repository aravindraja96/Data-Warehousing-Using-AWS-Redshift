import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events;"
staging_songs_table_drop = "drop table IF EXISTS staging_songs;"
songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = "drop table IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
create table IF NOT EXISTS staging_events(
artist varchar,auth varchar,firstname varchar,
gender char,itemInSession int,lastname varchar,
length float,level varchar,location varchar,method varchar,page varchar,registration float,
sessionid int,song varchar,status int,ts bigint,useragent varchar,userid int);

""")

staging_songs_table_create = ("""
create table IF NOT EXISTS staging_songs(
num_songs int,artist_id varchar,location varchar,latitude numeric,longitude numeric,name varchar,
song_id varchar,title varchar,duration float,year int
);

""")

songplay_table_create = ("""
create table IF NOT EXISTS songplays (songplay_id int IDENTITY(0,1),start_time timestamp NOT NULL,user_id int NOT NULL,level varchar,song_id varchar NOT NULL,artist_id varchar NOT NULL,session_id int,location varchar,user_agent varchar);
""")

user_table_create = ("""
create table IF NOT EXISTS users (user_id int,first_name varchar,last_name varchar,gender char(1),level varchar);
""")

song_table_create = ("""
create table IF NOT EXISTS songs (song_id varchar PRIMARY KEY ,title varchar,artist_id varchar,year integer,duration numeric);
""")

artist_table_create = ("""
create table IF NOT EXISTS artists (artist_id varchar Primary Key,name varchar,location varchar,latitude numeric,longitude numeric);
""")

time_table_create = ("""
create table IF NOT EXISTS time(start_time timestamp primary key,hour int,day int,week int,month int,year int,weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
 copy staging_events from 's3://udacity-dend/log_data' 
credentials 'aws_iam_role={}' compupdate off
 region 'us-west-2'
  JSON 's3://udacity-dend/log_json_path.json';
""").format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
copy staging_songs from 's3://udacity-dend/song_data/A/A/A' 
credentials 'aws_iam_role={}' compupdate off
 region 'us-west-2'
 JSON 'auto' ;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent )
SELECT distinct
TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' as start_time,
events.userid as user_id,
events.level as level,
songs.song_id as song_id,
songs.artist_id AS artist_id,
events.sessionid AS session_id,
events.location AS location,
events.useragent AS user_agent
FROM staging_events AS events
JOIN staging_songs AS songs
ON (events.artist = songs.name)
AND (events.song = songs.title)
AND (events.length = songs.duration)
WHERE events.page = 'NextSong' and events.userid is not null
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level )
select distinct  userid, firstname, lastname, gender, level from staging_events WHERE page = 'NextSong' and userid is not null;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration )
select distinct song_id, title, artist_id, year, duration  from staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists ( artist_id, name, location, latitude, longitude )
select distinct  artist_id,name,location,latitude,longitude from  staging_songs;
""")



time_table_insert = ("""
INSERT INTO time(start_time,hour,day,week,month,year,weekday)
select distinct s.start_time,
extract(hour from s.start_time) ,
extract(day from s.start_time),
extract(week from s.start_time),
extract(month from s.start_time),
extract(year from s.start_time),
extract(weekday from s.start_time) from songplays s;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
