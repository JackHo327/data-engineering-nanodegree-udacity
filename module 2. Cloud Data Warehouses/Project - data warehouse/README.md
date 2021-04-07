## Data Warehouse

This README will illustrate the context/purpose of this porject and the justification on the database schema design (might be subjective ;p).

### Purpose

Sparkify, a new startup, launched its brand new music streaming App, and it collected a lot of data related to the user activities on the App. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. In order to get a more fine-grind analytics on what kind of songs their users were listening to, Sparkify decided to create a database datawarehosue using AWS Redshift to allow analyst to easily query the records and do analytics.

### Justification on the DB Schema
The way I think the whole workflow should be 2 major parts:

- Load the raw data (S3) into staging tables (AWS Redshift)
- Process the raw data in the staging tables and load them into corresponding fact and dimensional tables

![foo](exploratory_resources/datawarehouse%20project%20workflow.png)

#### Staging Tables
There are two staging tables: `staging_events` and `staging_songs`.

_\*Masked my credentials in below `COPY` commands._

- staging_events
    - schema: add a compound sortkey that combines `song`, `artist` and `length`, because these three keys will be used in the `WHERE` caluses when produced fact and dimensional tables
  ```sql
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
  ```
    - load into staging table: set `timeformat 'epochmillisecs'` to make sure no issues when loading `ts` column as timestamp values; set `COMPUPDATE OFF STATUPDATE OFF` to loose the checks/updates and speed up the load a little bit.
  ```bash
  copy staging_events from 's3://udacity-dend/log_data/' credentials 'aws_iam_role=arn:aws:iam::**********:role/dwhRole' json 's3://udacity-dend/log_json_path.json' timeformat 'epochmillisecs' region 'us-west-2' COMPUPDATE OFF STATUPDATE OFF;
  ```

- staging_songs

    - schema: add a compound sortkey that combines `song_id` and `artist_id`, because these three keys will be used in the `WHERE` caluses when produced fact and dimensional tables
  ```sql
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
  ```
    - load into staging table: similar settings as the copy command for staging_events
  ```bash
  copy staging_songs from 's3://udacity-dend/song_data/' credentials 'aws_iam_role=arn:aws:iam::**********:role/dwhRole' json 'auto' region 'us-west-2' COMPUPDATE OFF STATUPDATE OFF;
  ```

#### Fact and Dimensional Tables

There are four dimensional tables: `artists`, `songs`, `users` and `time`; One and the only fact table `songplays`

- artists

    - schema: set as `diststyle all` and sort by artist_id for potential queries
  ```sql
    CREATE TABLE artists (
            artist_id varchar(256) sortkey,
            name varchar(256),
            location text,
            latitude double precision,
            longitude double precision
        )
    diststyle all;
  ```
    - insert into artists table: all the values will be directly come from staging_songs table, records should be unique
  ```sql
    insert into artists
    (select distinct artist_id, 
        artist_name as name,
        artist_location as location,
        artist_latitude as latitude,
        artist_longitude as longitude
    from staging_songs);
  ```

- songs

    - schema: set as `diststyle all` and sort by song_id for potential queries
  ```sql
    CREATE TABLE songs (
            song_id varchar(256) sortkey,
            title varchar(256),
            artist_id varchar(256),
            year integer,
            duration double precision
        )
    diststyle all;
  ```
    - insert into songs table: all the values will be directly come from staging_songs table, records should be unique
  ```sql
    insert into songs
    (select distinct song_id, 
        title,
        artist_id,
        year,
        duration
    from staging_songs);
  ```

- users

    - schema: set as `diststyle all` and sort by user_id for potential queries
  ```sql
    CREATE TABLE users (
            user_id varchar(256) sortkey,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            level varchar(256)
        )
    diststyle all;
  ```
    - insert into users table: all the values will be directly come from staging_events table, records should be unique and only filter the `staging_events.page='NextSong'` records from staging_events table
  ```sql
    insert into users
    (select distinct userId as user_id, 
        firstName  as first_name, 
        lastName as last_name,
        gender,
        level
    from staging_events 
    where staging_events.page='NextSong');
  ```

- time

    - schema: set as `diststyle all` and sort by start_time for potential queries
  ```sql
    CREATE TABLE time (
            start_time timestamp sortkey,
            hour integer,
            day integer,
            week integer,
            month integer,
            year integer,
            weekday varchar(256)
        ) 
    diststyle all;
  ```
    - insert into time table: all the values will be directly come from staging_events table, records should be unique and only filter the `staging_events.page='NextSong'` records from staging_events table
  ```sql
    insert into time
    (select distinct ts as start_time, 
        date_part(hr,ts) as hour,
        date_part(d,ts) as day,
        date_part(w,ts) as week,
        date_part(mon,ts) as month,
        date_part(y,ts) as year,
        CASE 
        WHEN date_part(dow,ts) = 0 THEN 'Sunday'
        WHEN date_part(dow,ts) = 1 THEN 'Monday'
        WHEN date_part(dow,ts) = 2 THEN 'Tuesday'
        WHEN date_part(dow,ts) = 3 THEN 'Wednesday'
        WHEN date_part(dow,ts) = 4 THEN 'Thursday'
        WHEN date_part(dow,ts) = 5 THEN 'Friday'
        ELSE 'Saturday' 
        END as weekday
    from staging_events
    where staging_events.page='NextSong');
  ```

- songplays

    - schema: set `songplay_id` as the distkey and sort by combined vlaues (`start_time`, `user_id`, `song_id`) for potential queries
  ```sql
    CREATE TABLE songplays (
        songplay_id integer IDENTITY(0,1) distkey,
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
  ```
    - insert into songplays table: most of values will be directly come from staging_events table; `song_id` and `artist_id` will come from `songs` and `artists` table; records should be filtered by the `staging_events.page='NextSong'`
  ```sql
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
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
  ```

### Instructions on How to Run the Project
Kick off these two commands below, and all the tables will be created and loaded into a Redshift DB.

_\*Code for creating instances has not been included._

```txt
> python3 create_tables.py
> python3 etl.py
```

### Example Queries

Please check the `test.ipynb`.
