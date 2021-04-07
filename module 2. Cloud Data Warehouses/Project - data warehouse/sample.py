import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE staging_events (
        artist varchar(256),
        auth    varchar(256),
        firstName   varchar(256),
        gender  varchar(256),
        itemInSession integer,
        lastName    varchar(256),
        length   double precision,
        level   varchar(256),
        location    text,
        method  varchar(256),
        page    varchar(256),
        registration    double precision,
        sessionId   integer,
        song    varchar(256),
        status  integer,
        ts  timestamp,
        userAgent   text,
        userId  varchar(256)
    ) 
sortkey(song, artist, length);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs (
        num_songs   integer,
        artist_id   varchar(256),
        artist_latitude double precision,
        artist_longitude    double precision,
        artist_location text,
        artist_name varchar(256),
        song_id varchar(256),
        title   varchar(256),
        duration   double precision,
        year    integer
    )
sortkey(song_id, artist_id);
""")

songplay_table_create = ("""CREATE TABLE songplays (
        songplay_id integer IDENTITY(0,1) primary key distkey,
        start_time timestamp not null references time(start_time),
        user_id varchar(256) not null references users(user_id),
        level varchar(256),
        song_id varchar(256) not null references songs(song_id),
        artist_id varchar(256) not null references artists(artist_id),
        session_id integer,
        location text,
        user_agent text
    ) 
sortkey(start_time, user_id, song_id);
""")

user_table_create = ("""CREATE TABLE users (
        user_id varchar(256) primary key sortkey,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        level varchar(256)
    )
diststyle all;
""")

song_table_create = ("""CREATE TABLE songs (
        song_id varchar(256) primary key sortkey,
        title varchar(256),
        artist_id varchar(256),
        year integer,
        duration double precision
    )
diststyle all;
""")

artist_table_create = ("""CREATE TABLE artists (
        artist_id varchar(256) primary key sortkey,
        name varchar(256),
        location text,
        latitude double precision,
        longitude double precision
    )
diststyle all;
""")

time_table_create = ("""CREATE TABLE time (
        start_time timestamp primary key sortkey,
        hour integer,
        day integer,
        week integer,
        month integer,
        year integer,
        weekday varchar(256)
    ) 
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {_log_data_} credentials 'aws_iam_role={_iam_role_}' json {_log_json_path_file_} timeformat 'epochmillisecs' region 'us-west-2' COMPUPDATE OFF STATUPDATE OFF;
""").format(_log_data_=config['S3']['LOG_DATA'],
            _iam_role_=config['IAM_ROLE']['ARN'],
            _log_json_path_file_=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""copy staging_songs from {_song_data_} credentials 'aws_iam_role={_iam_role_}' json 'auto' region 'us-west-2' COMPUPDATE OFF STATUPDATE OFF;
""").format(_song_data_=config['S3']['SONG_DATA'],
            _iam_role_=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    (select ts as start_time,
        userId as user_id,
        level,
        s.song_id as song_id, 
        a.artist_id as artist_id,
        sessionId as session_id,
        se.location as location,
        userAgent as user_agent
    from staging_events se, songs s, artists a
    where se.song = s.title and se.artist = a.name and s.duration = se.length and se.page='NextSong');
""")

user_table_insert = ("""insert into users
    (select distinct userId as user_id, 
        firstName  as first_name, 
        lastName as last_name,
        gender,
        level
    from staging_events 
    where staging_events.page='NextSong');
""")

song_table_insert = ("""insert into songs
    (select distinct song_id, 
        title,
        artist_id,
        year,
        duration
    from staging_songs);
""")

artist_table_insert = ("""insert into artists
    (select distinct artist_id, 
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs);
""")

time_table_insert = ("""insert into time
    (select distinct start_time as start_time, 
        date_part(hr,start_time) as hour,
        date_part(d,start_time) as day,
        date_part(w,start_time) as week,
        date_part(mon,start_time) as month,
        date_part(y,start_time) as year,
        CASE 
            WHEN date_part(dow,start_time) = 0 THEN 'Sunday'
            WHEN date_part(dow,start_time) = 1 THEN 'Monday'
            WHEN date_part(dow,start_time) = 2 THEN 'Tuesday'
            WHEN date_part(dow,start_time) = 3 THEN 'Wednesday'
            WHEN date_part(dow,start_time) = 4 THEN 'Thursday'
            WHEN date_part(dow,start_time) = 5 THEN 'Friday'
            ELSE 'Saturday' 
            END as weekday
    from songplays);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, artist_table_insert,
                        songplay_table_insert, user_table_insert, time_table_insert]
