# import findspark
# findspark.init()

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import udf, col, row_number
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, LongType, MapType

config = configparser.ConfigParser()
config.read('dl.cfg')

# os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
# os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a spark session."""

    spark = SparkSession \
        .builder \
        .appName('de nanodegree datalake proj') \
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:2.7.0') \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """Process song data.

    Args:
        spark (SparkSession): spark session object
        input_data (str): input directory
        output_data (str): output directory
    """
    # get filepath to song data file
    song_data = input_data + "*/*/*/*.json"

    song_data_schema = StructType([
        StructField('artist_id', StringType()),
        StructField('artist_latitude', StringType()),
        StructField('artist_location', StringType()),
        StructField('artist_longitude', StringType()),
        StructField('artist_name', StringType()),
        StructField('duration', DoubleType()),
        StructField('num_songs', IntegerType()),
        StructField('song_id', StringType()),
        StructField('title', StringType()),
        StructField('year', IntegerType())
    ]
    )

    # read song data file
    df = spark.read.json(song_data, schema=song_data_schema)
    df.createOrReplaceTempView('raw_song_table')

    # extract columns to create songs table
    songs_table = spark.sql('''SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
FROM raw_song_table
''')

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').mode(
        'overwrite').parquet(output_data+'songs.parquet')

    # extract columns to create artists table
    artists_table = spark.sql('''SELECT DISTINCT artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS lattitude,
    artist_longitude AS longitude
FROM raw_song_table
''')

    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(
        output_data+'artists.parquet')


def process_log_data(spark, input_data, output_data):
    """Process log data.

    Args:
        spark (SparkSession): spark session object
        input_data (str): input directory
        output_data (str): output directory
    """
    # get filepath to log data file
    log_data = input_data+"*/*/*.json"

    log_data_schema = StructType([
        # StructField('payment_id',IntegerType()),
        StructField('artist', StringType()),
        StructField('auth', StringType()),
        StructField('firstName', StringType()),
        StructField('gender', StringType()),
        StructField('itemInSession', IntegerType()),
        StructField('lastName', StringType()),
        StructField('length', DoubleType()),
        StructField('level', StringType()),
        StructField('location', StringType()),
        StructField('method', StringType()),
        StructField('page', StringType()),
        StructField('registration', DoubleType()),
        StructField('sessionId', StringType()),
        StructField('song', StringType()),
        StructField('status', IntegerType()),
        StructField('ts', LongType()),
        StructField('userAgent', StringType()),
        StructField('userId', StringType())
    ])

    # read log data file
    df = spark.read.json(log_data, schema=log_data_schema)

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
    time_table_exprs = ["resolve_ts['{}'] as {}".format(
        field, field) for field in time_table_fields]
    time_table = time_table.selectExpr(*time_table_exprs)

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').mode(
        'overwrite').parquet(output_data+'time.parquet')

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(
        output_data+'songs.parquet').select('song_id', 'title', 'duration')
    artists_df = spark.read.parquet(
        output_data+'artists.parquet').select('artist_id', 'name')

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.select(col('ts'), col('userId').alias('user_id'), col('level'), col('song'),col('length'),col('artist'),
                                col('sessionId').alias('session_id'), col('location'), col('userAgent').alias('user_agent'))

    songplays_table = songplays_table.join(songs_df, (songplays_table.song == songs_df.title) & (songplays_table.length == songs_df.duration), 'inner')\
        .join(artists_df, songplays_table.artist == artists_df.name, 'inner') \
        .select('ts', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent')

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


def main():
    """main function of this script."""
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'

    # use the existing s3 bucket
    output_data = f"{config['AWS']['AWS_S3_BUCKET']}/"

    song_input_data = input_data+"song_data/"
    process_song_data(spark, song_input_data, output_data)

    log_input_data = input_data+"log_data/"
    process_log_data(spark, log_input_data, output_data)

    spark.stop()


if __name__ == '__main__':
    main()
