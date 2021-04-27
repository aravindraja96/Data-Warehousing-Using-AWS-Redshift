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
CREATE TABLE IF NOT EXISTS staging_events
  (
     artist        VARCHAR,
     auth          VARCHAR,
     firstname     VARCHAR,
     gender        CHAR,
     iteminsession INT,
     lastname      VARCHAR,
     length        FLOAT,
     level         VARCHAR,
     location      VARCHAR,
     method        VARCHAR,
     page          VARCHAR,
     registration  VARCHAR,
     sessionid     INT,
     song          VARCHAR,
     status        INT,
     ts            BIGINT,
     useragent     VARCHAR,
     userid        INT
  ); 

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
  (
     num_songs INT,
     artist_id VARCHAR,
     location  VARCHAR,
     latitude  NUMERIC,
     longitude NUMERIC,
     name      VARCHAR,
     song_id   VARCHAR,
     title     VARCHAR,
     duration  FLOAT,
     year      INT
  ); 

""")

songplay_table_create = ("""
CREATE TABLEIF NOT EXISTS songplays 
    (
        songplay_id INT IDENTITY(0,1),
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id INT,
        location VARCHAR,
        user_agent VARCHAR
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
  (
     user_id    INT,
     first_name VARCHAR,
     last_name  VARCHAR,
     gender     CHAR(1),
     level      VARCHAR
  ); 
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
  (
     song_id   VARCHAR PRIMARY KEY,
     title     VARCHAR,
     artist_id VARCHAR,
     year      INTEGER,
     duration  NUMERIC
  ); 
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
  (
     artist_id VARCHAR PRIMARY KEY,
     name      VARCHAR,
     location  VARCHAR,
     latitude  NUMERIC,
     longitude NUMERIC
  ); """)

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
  (
     start_time TIMESTAMP PRIMARY KEY,
     hour       INT,
     day        INT,
     week       INT,
     month      INT,
     year       INT,
     weekday    INT
  );""")

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
INSERT INTO songplays
            (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
            )
SELECT DISTINCT timestamp 'epoch' + events.ts/1000 * interval '1 second' AS start_time,
                events.userid                                            AS user_id,
                events.level                                             AS level,
                songs.song_id                                            AS song_id,
                songs.artist_id                                          AS artist_id,
                events.sessionid                                         AS session_id,
                events.location                                          AS location,
                events.useragent                                         AS user_agent
FROM            staging_events                                           AS events
JOIN            staging_songs                                            AS songs
ON              (
                                events.artist = songs.NAME)
AND             (
                                events.song = songs.title)
AND             (
                                events.length = songs.duration)
WHERE           events.page = 'NextSong'
AND             events.userid IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users
            (user_id,
             first_name,
             last_name,
             gender,
             level)
SELECT DISTINCT userid,
                firstname,
                lastname,
                gender,
                level
FROM   staging_events
WHERE  page = 'NextSong'
       AND userid IS NOT NULL
       AND user_id NOT IN (SELECT DISTINCT user_id FROM users); 
""")

song_table_insert = ("""
INSERT INTO songs
            (song_id,
             title,
             artist_id,
             year,
             duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM   staging_songs; 
""")

artist_table_insert = ("""
INSERT INTO artists
            (artist_id,
             NAME,
             location,
             latitude,
             longitude)
SELECT DISTINCT artist_id,
                NAME,
                location,
                latitude,
                longitude
FROM   staging_songs; 
""")



time_table_insert = ("""
INSERT INTO time
            (start_time,
             hour,
             day,
             week,
             month,
             year,
             weekday)
SELECT DISTINCT s.start_time,
                Extract(hour FROM s.start_time),
                Extract(day FROM s.start_time),
                Extract(week FROM s.start_time),
                Extract(month FROM s.start_time),
                Extract(year FROM s.start_time),
                Extract(weekday FROM s.start_time)
FROM   songplays s; 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
