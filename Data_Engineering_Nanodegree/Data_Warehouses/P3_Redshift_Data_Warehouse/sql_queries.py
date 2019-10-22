import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE STAGING TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events ( 
    
    artist VARCHAR,
    auth VARCHAR,
    firstname VARCHAR,
    gender VARCHAR,
    iteminsession VARCHAR,
    lastName VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration DECIMAL,
    sessionId VARCHAR,
    song VARCHAR,
    status VARCHAR,
    ts DOUBLE PRECISION,
    userAgent VARCHAR,
    userId VARCHAR);
    
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(

   num_songs NUMERIC,
   artist_id VARCHAR,
   artist_latitude DOUBLE PRECISION,
   artist_longitude DOUBLE PRECISION, 
   artist_location VARCHAR,
   artist_name VARCHAR,
   song_id VARCHAR,
   title VARCHAR, 
   duration DOUBLE PRECISION,
   year DECIMAL


)""")


# STAGING TABLES COPY DATA FROM S3 INTO STAGING TABLES 


staging_events_copy = ("""
   
    copy staging_events 
    from 's3://udacity-dend/log_data'
    iam_role {}
    FORMAT as json 's3://udacity-dend/log_json_path.json'  REGION 'us-west-2';
   
    """).format(config.get('IAM_ROLE', 'ARN'))

staging_songs_copy = ("""
    
    copy staging_songs
    from 's3://udacity-dend/song_data'
    iam_role {}
    format as json 'auto';
""").format(config.get('IAM_ROLE', 'ARN'))

# CREATE STAR SCHEMA TABLES 

user_table_create = ("""CREATE TABLE user_table (
    
    userid VARCHAR,
    firstname VARCHAR,
    lastname VARCHAR,
    gender VARCHAR,
    level VARCHAR,
    PRIMARY KEY(userid))
    
""")

artist_table_create = (""" CREATE TABLE artist_table (
    artist_id VARCHAR,
    name VARCHAR,
    location VARCHAR,
    lattitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION)
    
""")


songplay_table_create = ("""CREATE TABLE songplay_table (

    songplay_id INTEGER IDENTITY(0,1),
    start_time DOUBLE PRECISION, 
    user_id VARCHAR, 
    level VARCHAR,
    song_id VARCHAR ,
    artist_id VARCHAR, 
    sessionId VARCHAR, 
    location VARCHAR, 
    user_agent VARCHAR)
""")


song_table_create = (""" CREATE TABLE song_table ( 
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year DECIMAL,
    duration DOUBLE PRECISION)
    
""")



time_table_create = ("""CREATE TABLE time_table (

    start_time DOUBLE PRECISION,
    hour DOUBLE PRECISION,
    day DOUBLE PRECISION,
    week DOUBLE PRECISION,
    month DOUBLE PRECISION,
    year DOUBLE PRECISION
    )
        
""")


# INSERT DATA INTO STAR SCHEMA TABLES  FINAL TABLES

#Reference Tables

user_table_insert = (""" insert into user_table 
SELECT
    DISTINCT(userid) as userid,
    firstname,
    lastname,
    gender,
    level
    FROM staging_events
    WHERE page = 'NextSong';
    
""")

artist_table_insert = (""" insert into artist_table 
SELECT
    DISTINCT (artist_id),
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM 
    staging_songs;
   
""")

song_table_insert = (""" insert into song_table
SELECT
    DISTINCT(song_id),
    title,
    artist_id,
    year,
    duration
FROM staging_songs;

""")

time_table_insert = (""" insert into time_table
SELECT 
    distinct(ts),
    round((ts/3600000), -1),
    round((ts/86400000), -1),
    round((ts/604800000), -1),
    round((ts/2592000000), -1),
    round((ts/31536000000), -1) 
FROM staging_events;   

""")

# Fact Table

songplay_table_insert = ("""insert into songplay_table(
    start_time, 
    user_id, 
    level,
    song_id,
    artist_id, 
    sessionId, 
    location, 
    user_agent)
    SELECT 
        staging_events.ts,
        staging_events.userId, 
        staging_events.level,
        staging_songs.song_id,
        staging_songs.artist_id, 
        staging_events.sessionId, 
        staging_events.location, 
        staging_events.useragent
    FROM staging_events, staging_songs
    WHERE staging_events.page = 'NextSong';      

""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, artist_table_insert, song_table_insert, time_table_insert]

