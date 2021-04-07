class SqlQueries:
    
    staging_events_table_create = ("""
        DROP TABLE IF EXISTS staging_events;

        CREATE TABLE staging_events (
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
    
    staging_songs_table_create = ("""
        DROP TABLE IF EXISTS staging_songs;

        CREATE TABLE staging_songs (
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

    copy_staging_table = ("""
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        json '{}'
        timeformat 'epochmillisecs' 
        region 'us-west-2' 
        compupdate off 
        statupdate off;
    """)
    
    songplays_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id text distkey,
            start_time timestamp NOT NULL,
            user_id varchar(256) NOT NULL,
            level varchar(256),
            song_id varchar(256) NOT NULL,
            artist_id varchar(256) NOT NULL,
            session_id integer,
            location text,
            user_agent text
        ) 
        sortkey(start_time, user_id, song_id);

    """
    )

    songplays_table_insert = ("""
        INSERT INTO songplays (
            SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT ts AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
        );
    """
    )
    
    users_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
          user_id varchar(256) sortkey,
          first_name varchar(256),
          last_name varchar(256),
          gender varchar(256),
          level varchar(256)
        )
        diststyle all;               
    """
    )
    
    users_table_insert = ("""
        INSERT INTO users(
            SELECT distinct userid, firstname, lastname, gender, level
            FROM staging_events
            WHERE page='NextSong'
        );
    """
    )

    songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
          song_id varchar(256) sortkey,
          title varchar(256),
          artist_id varchar(256),
          year integer,
          duration double precision
        )
        diststyle all;
    """    
    )
    
    songs_table_insert = ("""
        INSERT INTO songs(
            SELECT distinct song_id, title, artist_id, year, duration
            FROM staging_songs
        );
    """
    )
    
    artists_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
                artist_id varchar(256) sortkey,
                name varchar(256),
                location text,
                latitude double precision,
                longitude double precision
        )
        diststyle all;                           
    """
    )
    
    artists_table_insert = ("""
        INSERT INTO artists(
            SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
            FROM staging_songs
        );
    """
    )

    
    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
                start_time timestamp sortkey,
                hour integer,
                day integer,
                week integer,
                month integer,
                year integer,
                weekday varchar(256)
        )
        diststyle all;                         
    """
    )
    
    time_table_insert = ("""
        INSERT INTO time(
            SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
                   extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
            FROM songplays
        );
    """
    )
        