# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id VARCHAR(256) NOT NULL,
    level VARCHAR(10),
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    session_id INT,
    location VARCHAR(256),
    user_agent TEXT
);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(256) PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    gender VARCHAR(10),
    level VARCHAR(10)
);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(128),
    location VARCHAR(256),
    latitude NUMERIC,
    longitude NUMERIC
);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(256),
    year INT,
    duration NUMERIC
);
""")



time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR(20)
);
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s)  ON CONFLICT (user_id)
DO UPDATE
    SET level=EXCLUDED.level;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)  VALUES(%s, %s, %s, %s, %s) ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time)
DO NOTHING;
""")

# FIND SONGS

song_select = ("""SELECT song_id, a.artist_id
FROM songs s, artists a
WHERE s.artist_id = a.artist_id 
    AND s.title = %s
    AND a.name = %s 
    AND s.duration = %s 
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]