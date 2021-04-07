## Data Modeling with Postgres

This README will illustrate the context/purpose of this porject and the justification on the database schema design (might be subjective ;p).

### Purpose

Sparkify, a new startup, launched its brand new music streaming App, and it collected a lot of data related to the user activities on the App. In order to get a more fine-grind analytics on what kind of songs their users were listening to, Sparkify decided to create a database schema that could allow analyst to easily query the records and do analytics.

### Justification on the DB Schema

There are five tables needed to be designed in terms of `CREATE TABLE` statements and `INSERT INTO` statements. Let's review each one of them.

- `users` Table

`users` table contains 5 fields: `user_id`, `first_name`, `last_name`, `gender` and `level`. These fields all come from log/event files, which means there might be duplicates. 

However, I think I just want to keep the latest user info, and make the `users` table as uniqe by `user_id`, then which means when doing `INSERT`, I need to keep updating the other fields. Here I made an important assumption: 
> I assume the log file `2018-11-01-events` only contains logs earlier than `2018-11-02-events`, and apply to other files. Meanwhile, load these files from earliest to the latest by sort the `all_files` list and sort the dataframe before I trim the log data to user dataframe, so that I could always keep the latest status for users in `users` table.

```SQL
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(256) PRIMARY KEY,
    first_name VARCHAR(128),
    last_name VARCHAR(128),
    gender VARCHAR(10),
    level VARCHAR(10)
);

INSERT INTO users(user_id, first_name, 
                    last_name, gender, level) 
VALUES(%s, %s, %s, %s, %s)  
ON CONFLICT (user_id)
DO UPDATE
    SET first_name = %s, 
    last_name = %s, 
    gender = %s, 
    level = %s;
```

- `songs` Table

`songs` table contains 5 fields: `song_id`, `title`, `artist`, `year` and `duration`. These fields all come from songs files - these records should be unique in terms of song_id.

```SQL
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(256) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(256),
    year INT,
    duration NUMERIC
);

INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES(%s, %s, %s, %s, %s) 
ON CONFLICT (song_id) 
DO NOTHING;
```

- `artists` Table

`artists` table contains 5 fields: `artist_id`, `name`, `location`, `latitude` and `longitude`. These fields all come from songs files - these raw records  will not be unique in terms of artist_id, since one artist can release multiple songs. When load records into `artists`, if there is a conflict on `artist_id`, the `INSERT` will DO NOTHING since the `artist_id` should be unique (might be able to set it as DO UPDATE since the demograpgic info might change, but it will not affect too much on the core analytics, I think).

```SQL
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(256) PRIMARY KEY,
    name VARCHAR(128),
    location VARCHAR(256),
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO artists(artist_id, name, 
            location, latitude, longitude) 
VALUES(%s, %s, %s, %s, %s) 
ON CONFLICT (artist_id) 
DO NOTHING;
```

- `time` Table

`time` table contains 7 fields: `start_time`, `hour`, `day`, `week`, `month`, `year` and `weekday`. Will need to parse them from the `ts` field in log/event files, so there might be duplicate records - since multiple users could use the App at the same time. However, in the time table, there should be unique records to represent each `ts`.

```SQL
CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR(20)
);

INSERT INTO time(start_time, hour, day, 
                week, month, year, weekday) 
VALUES(%s, %s, %s, %s, %s, %s, %s) 
ON CONFLICT (start_time)
DO NOTHING;
```

- `songplays` Table

This table just simple followed the instruction in the project description by:
1. Firstly, find out the `song_id` and `artist_id` from above `songs` and `artists` tables
2. Then, insert the above 2 fields into `songplays` table with other existing fileds in the log/event files

** made songplay_id as a SERIAL PRIMARY KEY.

```SQL
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

INSERT INTO songplays(start_time, user_id, level, 
        song_id, artist_id, session_id, 
        location, user_agent) 
VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
```

### Instructions on How to Run the Project

To run this project, there are mainly two steps to do:

- Set up the database and create the relevant tables.

** Run the command below in the terminal.

```bash
> python3 create_tables.py
````

- Execute the etl.py to process the raw data and load them into relevant tables.

** Run the command below in the terminal.

```bash
> python3 etl.py
```

After you finished above two steps, you could check the results by either running the code in `test.ipnb` or run SQLs after you login the postgres with similar commands in below `Example Queries` section.

### Example Queries

Assume that you have set up all the necessary tools and run the commands in previous section, you could know check records in the postgres.

```bash
root@60bdd35418d8:/home/workspace# psql --host=127.0.0.1 --dbname=sparkifydb --user=student --password
Password for user student: 
psql (9.5.23)
SSL connection (protocol: TLSv1.2, cipher: ECDHE-RSA-AES256-GCM-SHA384, bits: 256, compression: off)
Type "help" for help.

sparkifydb=# SELECT * FROM songplays WHERE artist_id IS NOT NULL AND song_id IS NOT NULL;
 songplay_id |       start_time        | user_id | level |      song_id       |     artist_id      | session_id |              location              |                                                                user_agent                                
                                 
-------------+-------------------------+---------+-------+--------------------+--------------------+------------+------------------------------------+----------------------------------------------------------------------------------------------------------
---------------------------------
        4627 | 2018-11-21 21:56:47.796 | 15      | paid  | SOZCTXZ12AB0182364 | AR5KOSW1187FB35FF4 |        818 | Chicago-Naperville-Elgin, IL-IN-WI | "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chr
ome/36.0.1985.125 Safari/537.36"
(1 row)
```