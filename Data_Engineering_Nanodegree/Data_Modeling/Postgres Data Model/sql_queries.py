# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay_table;"
user_table_drop = "DROP TABLE IF EXISTS user_table;"
song_table_drop = "DROP TABLE IF EXISTS song_table;"
artist_table_drop = "DROP TABLE IF EXISTS artist_table;"
time_table_drop = "DROP TABLE IF EXISTS time_table;"

# CREATE TABLES


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay_table (
        songplay_id SERIAL PRIMARY KEY, 
        ts timestamp       NOT NULL, 
        userID varchar     NOT NULL, 
        level varchar, 
        songid varchar, 
        artistid varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table (
        user_id int PRIMARY KEY,
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table (  
        song_id varchar PRIMARY KEY,
        title varchar,
        artist_id varchar   NOT NULL,
        year int, duration int); 
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table (
        artist_id varchar PRIMARY KEY,
        name varchar,
        location varchar,
        latitude decimal,
        longitude decimal);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table (
        serial_time_id SERIAL PRIMARY KEY,
        start_time timestamp, 
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday int); 
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplay_table (ts, userID, level, songid, artistid, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);""")

user_table_insert = ("""INSERT INTO user_table (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) on conflict(user_id) do update set level=excluded.level
;""")

song_table_insert = ("""INSERT INTO song_table (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

artist_table_insert = ("""INSERT INTO artist_table (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

time_table_insert = ("""INSERT INTO time_table (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING;""")

# FIND SONGS
#Implement the song_select query in sql_queries.py to find the song ID and artist ID based on the title, artist name, and #duration of a song.

song_select = ("""SELECT song_table.song_id, artist_table.artist_id FROM song_table JOIN artist_table on song_table.artist_id = artist_table.artist_id WHERE title LIKE %s AND artist_table.artist_id LIKE %s AND duration = %s;
""")
#(title, artist_id, duration)

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

