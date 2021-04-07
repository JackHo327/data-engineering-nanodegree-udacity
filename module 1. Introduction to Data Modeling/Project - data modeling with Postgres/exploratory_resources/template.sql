-- tempalte file for writing SQLs
-- mainly for doing reformat


-- CREATE TABLES
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id SERIAL PRIMARY KEY,
    start_time timestamp,
    user_id VARCHAR(256),
    level VARCHAR(10),
    song_id VARCHAR(256),
    artist_id VARCHAR(256),
    session_id INT,
    location VARCHAR(256),
    user_agent TEXT
);

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(256) PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    gender VARCHAR(10),
    level VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(256) REFERENCES artists (artist_id),
    year INT,
    duration NUMERIC
);

CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(128),
    location VARCHAR(256),
    latitude NUMERIC,
    longitude NUMERIC
);

CREATE TABLE IF NOT EXISTS "time" (
    start_time timestamp,
    "hour" INT,
    "day" INT,
    "week" INT,
    "month" INT,
    "year" INT,
    "weekday" VARCHAR(20)
);


-- INSERT RECORDS INTO TABLES
INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s)

INSERT INTO artists(artist_id, name, location, latitude, longitude) VALUES(%s, %s, %s, %s, %s)

INSERT INTO time(start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s)

INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s)  ON CONFLICT (user_id) 
DO NOTHING;

INSERT INTO users(user_id, first_name, last_name, gender, level) VALUES(%s, %s, %s, %s, %s)

INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)

SELECT song_id, artist_id
FROM songs s, artists a
WHERE %s = %s and s.duration = %s