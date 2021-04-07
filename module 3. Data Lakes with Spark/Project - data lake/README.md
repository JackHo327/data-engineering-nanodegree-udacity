## Data Lake

This README will illustrate the context/purpose of this porject and the justification on the database schema design(might be subjective ;p).

### Purpose

Sparkify, a new startup, launched its brand new music streaming App, and it collected a lot of data related to the user activities on the App. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. In order to get a more fine-grind analytics on what kind of songs their users were listening to, Sparkify decided to create a data lake using AWS EMR and Spark(PySpark) to allow analyst to easily query the records and do analytics.

### Justification on the DB Schema
I think the whole workflow should be 3 major parts:

- Load the raw data from S3(`s3a: // udacity-dend /`) to EMR Cluster
- Process the raw data with Spark(pyspark)
- Store the analytical data back to S3(`config['AWS']['AWS_S3_BUCKET']` - use an existing bucket)

`songs` and `artists` parquet files could be directly created from raw song data files(`s3a: // udacity-dend/song_data/*/*/*/*.json`) - the idea is loading the raw data files and then create a temp view on the input data, and then extract the songs and artists by using `spark.sql()`.

- `songs` and `artists`

```python
df.createOrReplaceTempView('raw_song_table')

songs_table = spark.sql('''SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM raw_song_table
''')

songs_table.write.partitionBy('year', 'artist_id').mode(
    'overwrite').parquet(output_data+'songs.parquet')

artists_table = spark.sql('''SELECT DISTINCT artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS lattitude,
    artist_longitude AS longitude
FROM raw_song_table
''')

artists_table.write.mode('overwrite').parquet(
    output_data+'artists.parquet')
```

Meanwhile, the `users` and the `time` parquet files could basically follow the above logic and be created - the major difference is that the code pieces are use more heavily on the `DataFrame-related functions`, rather than still go with the `spark.sql()`, for example:

- `users` and `time`

```python
# filter by actions for song plays
df = df.filter(col('page') == 'NextSong')

# extract columns for users table
users_table = df.select('userId', 'firstName', 'lastName',
                        'gender', 'level', 'ts').sort(col('ts').desc())

# group by the users table by keeping the max timestamp - only keep the latest status of each user
users_table = users_table.groupBy(
    'userId', 'firstName', 'lastName', 'gender', 'level').max('ts').drop('max(ts)')

# write users table to parquet files
users_table.write.mode('overwrite').parquet(output_data+'users.parquet')

# extract columns to create time table - drop dups
time_table = df.select(col('ts').alias('raw_datetime')
                       ).dropDuplicates(['raw_datetime'])

# define this udf to help to parse the timestamp to different time granularities


@udf(MapType(StringType(), StringType()))
def resolve_ts(epoch_time):
    """Parse epoch timestamp to different time granularities."""

    weekday_dict = {7: 'Sunday', 1: 'Monday', 2: 'Tuesday',
                    3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday'}
    date_time_from_epoch = datetime.fromtimestamp(epoch_time/1000)

    return {'start_time': date_time_from_epoch.isoformat(sep=' ', timespec='milliseconds'),
            'hour': date_time_from_epoch.hour,
            'day': date_time_from_epoch.day,
            'week': date_time_from_epoch.isocalendar()[1],
            'month': date_time_from_epoch.month,
            'year': date_time_from_epoch.year,
            'weekday': weekday_dict[date_time_from_epoch.isoweekday()]}


time_table = time_table.withColumn(
    'resolve_ts', resolve_ts(time_table.raw_datetime))

time_table_fields = ['start_time', 'hour',
                     'day', 'week', 'month', 'year', 'weekday']
time_table_exprs = ['resolve_ts['{}'] as {}'.format(field, field) for field in time_table_fields]
time_table = time_table.selectExpr(*time_table_exprs)
```

For `songplays`, it requires to join `songs` and `artists` to get the relevant song_id and artists_id. Mmeanwhile, the `songs` and the `artitsts` data should have been wrote to S3, and in order to create `songplays` table, pyspark needs to read them back from S3.

```python
# read in song data to use for songplays table
songs_df = spark.read.parquet(
    output_data+'songs.parquet').select('song_id', 'title', 'duration')
artists_df = spark.read.parquet(
    output_data+'artists.parquet').select('artist_id', 'name')

# extract columns from joined song and log datasets to create songplays table
songplays_table = df.select(col('ts'), col('userId').alias('user_id'), col('level'), col('song'),
                            col('length'), col('artist'),
                            col('sessionId').alias(
                                'session_id'), col('location'), col
                            ('userAgent').alias('user_agent'))

songplays_table = songplays_table.join(songs_df, (songplays_table.song == songs_df.title) &
                                       (songplays_table.length == songs_df.duration), 'inner')\
    .join(artists_df, songplays_table.artist == artists_df.name, 'inner') \
    .select('ts', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location',
            'user_agent')

# process ts column so that it could be used to join with time table to get the year and month info
ts_to_str = udf(lambda x: datetime.fromtimestamp(
    x/1000).isoformat(sep=' ', timespec='milliseconds'))
songplays_table = songplays_table.withColumn('start_time', ts_to_str('ts'))

songplays_table = songplays_table.join(time_table, ['start_time'], 'inner')

win_row_num = Window().orderBy('start_time')
songplays_table = songplays_table.withColumn('songplay_id', row_number().over(win_row_num))\
    .select('songplay_id',
            'start_time', 'user_id',
            'level', 'song_id', 'artist_id',
            'session_id', 'location', 'user_agent',
            'year', 'month')


# write songplays table to parquet files partitioned by year and month
songplays_table.write.partitionBy('year', 'month').mode(
    'overwrite').parquet(output_data+'songplays.parquet')

```


### How to Run Script

First, need to create a EMR cluster.

```shell
> aws emr create-cluster --name "mySparkCluster" --use-default-roles --release-label emr-5.32.0 --applications Name=Spark Name=Hadoop Name=Hive Name=Livy Name=JupyterEnterpriseGateway --ec2-attributes KeyName=test-ec2,SubnetId=subnet-a49426dc --instance-type m5.xlarge --instance-count 5
{
    "ClusterId": "j-xxxxxxxxxxxxx",
    "ClusterArn": "arn:aws:elasticmapreduce:xxxxxxxxxxxxx:xxxxxxxxxxxxx:cluster/j-xxxxxxxxxxxxx"
}
```

Then move [etl.py](./etl.py) and [dl.cfg](./dl.cfg) file to the master node of EMR cluster(I stored these scripts on S3 just for convenience).

```bash
[hadoop@ip-172-31-21-67 ~]$ aws s3 cp s3://xxxxxxxxxxxxx/etl.py ./
download: s3: // xxxxxxxxxxxxx/etl.py to ./etl.py
[hadoop@ip-172-31-21-67 ~]$ aws s3 cp s3://xxxxxxxxxxxxx/dl.cfg ./
download: s3: // xxxxxxxxxxxxx/dl.cfg to ./dl.cfg
```

Submit a spark job.

```bash
[hadoop@ip-172-31-21-67 ~]$ spark-submit --master yarn ./etl.py
```

There should be some logs like:

```bash
....
yy/mm/dd HH: MM: SS INFO YarnClientSchedulerBackend: Stopped
yy/mm/dd HH: MM: SS INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
yy/mm/dd HH: MM: SS INFO MemoryStore: MemoryStore cleared
yy/mm/dd HH: MM: SS INFO BlockManager: BlockManager stopped
yy/mm/dd HH: MM: SS INFO BlockManagerMaster: BlockManagerMaster stopped
yy/mm/dd HH: MM: SS INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
yy/mm/dd HH: MM: SS INFO SparkContext: Successfully stopped SparkContext
yy/mm/dd HH: MM: SS INFO ShutdownHookManager: Shutdown hook called
yy/mm/dd HH: MM: SS INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-bf8863f6-ba7c-412c-9905-e3df9ce76642
yy/mm/dd HH: MM: SS INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-bf8863f6-ba7c-412c-9905-e3df9ce76642/pyspark-41455435-2377-4d2b-abeb-1f8eb2dd7fb5
yy/mm/dd HH: MM: SS INFO ShutdownHookManager: Deleting directory /mnt/tmp/spark-99f02ebd-89e8-4633-83c6-a32f89204594
```

### Inspect the Result
Made this [songplays](https://denanodegree-s3-bucket.s3-us-west-2.amazonaws.com/datalake_proj/songplays.parquet/year%3D2018/month%3D11/part-00000-76c46c19-a7da-42d3-8f4f-c90d3e5ccabb.c000.snappy.parquet) final result public temporarily.
