# This script contains four main functions 
# - each of them will provide the nessary data cleaning and transformation on the data to satisfy the scope of this capstone.
import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, LongType, MapType
from functools import reduce
from pyspark.sql import DataFrame as spark_DataFrame
import reverse_geocoder as rg

config = configparser.ConfigParser()
config.read('capstone.cfg')

os.environ["AWS_ACCESS_KEY_ID"] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """Create a spark session."""

    spark = SparkSession \
        .builder \
        .appName('de nanodegree capstone project') \
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11') \
        .getOrCreate()
    return spark


def etl_world_temperature(spark, input_dir, output_dir):
    """Clean the temperature data"""

    # load data
    data_input_full_file_path = f'{input_dir}/GlobalLandTemperaturesByCity.csv'
    world_temperature_spark_df = spark.read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(data_input_full_file_path)

    # just take temperature data after 2003-01-01 and only keep the US data
    world_temperature_spark_df = world_temperature_spark_df \
        .filter(F.col('dt') >= datetime(2003, 1, 1)) \
        .filter(F.col('Country') == 'United States')

    # parse month and day
    us_temperature_spark_df = world_temperature_spark_df \
        .withColumn('month', F.month(F.col('dt'))) \
        .withColumn('day', F.dayofmonth(F.col('dt'))) \
        .drop(F.col('dt'))

    # groupby columns and get the new avg temperature
    avg_us_temperature_spark_df = us_temperature_spark_df \
        .groupBy(['month', 'day', 'City', 'Country', 'Latitude', 'Longitude']) \
        .agg(F.mean('AverageTemperature')) \
        .withColumnRenamed('avg(AverageTemperature)', 'AverageTemperature') \
        .withColumn('month', F.col('month').cast('integer')) \
        .withColumn('day', F.col('day').cast('integer'))

    # covert DMS Lat and Lon to numeric format to get state info with an udf func
    avg_us_temperature_spark_df = avg_us_temperature_spark_df \
        .withColumn('Latitude', F.when(F.col('Latitude').rlike('N'), F.regexp_replace('Latitude', 'N', '').cast('double'))
                    .otherwise(-1*F.when(F.col('Latitude').rlike('N'), F.regexp_replace('Latitude', 'N', '').cast('double')))) \
        .withColumn('Longitude', F.when(F.col('Longitude').rlike('W'), -1 * F.regexp_replace('Longitude', 'W', '').cast('double'))
                    .otherwise(F.when(F.col('Longitude').rlike('W'), F.regexp_replace('Longitude', 'W', '').cast('double'))))

    # define a udf function to get state based on lat and lon by using reverse_geocoder library
    # https://github.com/thampiman/reverse-geocoder
    def _helper_get_state_(lat, lon):

        coor = (lat, lon)
        result = rg.search(coor)

        return result[0].get('admin1')

    _helper_get_state_udf = F.udf(
        lambda x, y: _helper_get_state_(x, y), StringType())

    avg_us_temperature_spark_df = avg_us_temperature_spark_df\
        .withColumn('state', _helper_get_state_udf(F.col('Latitude'), F.col('Longitude')))

    # load i94addr dictionary - map the i94addr values
    i94addr_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94addr_dictionary.csv'
    i94addr_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94addr_dictionary_input_full_file_path)

    i94addr_dictionary_spark_df = i94addr_dictionary_spark_df \
        .withColumn('init_cap_value', F.initcap(F.col('value')))

    avg_us_temperature_spark_df = avg_us_temperature_spark_df \
        .join(i94addr_dictionary_spark_df, avg_us_temperature_spark_df.state == i94addr_dictionary_spark_df.init_cap_value, 'left') \
        .drop('init_cap_value') \
        .drop('value') \
        .withColumnRenamed('key', 'state_code')

    avg_us_temperature_spark_df = avg_us_temperature_spark_df \
        .withColumnRenamed("Country", "country") \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("Latitude", "latitude") \
        .withColumnRenamed("Longitude", "longitude") \
        .withColumnRenamed("AverageTemperature", "avg_temperature")

    avg_us_temperature_spark_df = avg_us_temperature_spark_df \
        .withColumn('city_state_code', F.concat_ws(', ', F.upper(F.col('city')), F.upper(F.col('state_code'))))

    avg_us_temperature_spark_df = avg_us_temperature_spark_df.select(
        'month', 'day', 'city', 'state', 'state_code', 'city_state_code', 'avg_temperature').distinct()

    # output clean data
    data_output_full_file_path = f'{output_dir}/USCitiesTemperaturesByMonth.parquet'
    avg_us_temperature_spark_df \
        .write \
        .options(encoding="ISO-8859-1") \
        .mode('overwrite') \
        .parquet(data_output_full_file_path)


def etl_airport_code(spark, input_dir, output_dir):
    """Clean the airport code data"""

    # load data
    airport_code_data_input_full_file_path = f'{input_dir}/airport-codes_csv.csv'
    airport_code_spark_df = spark.read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(airport_code_data_input_full_file_path)

    airport_code_spark_df = airport_code_spark_df \
        .withColumnRenamed('name', 'airport_name') \
        .filter(F.col('iso_country') == 'US')

    # split iso_region column into Latitude and Longitude
    split_iso_region = F.split(airport_code_spark_df['iso_region'], '-')
    airport_code_spark_df = airport_code_spark_df \
        .withColumn('region', split_iso_region.getItem(1)) \
        .withColumn('municipality_region', F.concat_ws(', ', F.upper(F.col('municipality')), F.upper(F.col('region'))))

    new_airport_code_spark_df = airport_code_spark_df \
        .drop('iso_region') \
        .drop('coordinates')

    data_output_full_file_path = f'{output_dir}/airport-codes.parquet'
    new_airport_code_spark_df \
        .write \
        .options(encoding="ISO-8859-1") \
        .mode('overwrite') \
        .parquet(data_output_full_file_path)


def etl_us_cities_demographics(spark, input_dir, output_dir):
    """Clean the us cities demograpgics data"""
    # this data set is clean
    # load data
    data_input_full_file_path = f'{input_dir}/us-cities-demographics.csv'
    us_cities_demographics_spark_df = spark.read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1", sep=';') \
        .load(data_input_full_file_path)

    us_cities_demographics_spark_df = us_cities_demographics_spark_df \
        .withColumnRenamed("City", "city") \
        .withColumnRenamed("State", "state") \
        .withColumnRenamed("Median Age", "median_age") \
        .withColumnRenamed("Male Population", "male_population") \
        .withColumnRenamed("Female Population", "female_population") \
        .withColumnRenamed("Total Population", "total_population") \
        .withColumnRenamed("Number of Veterans", "num_of_veterans") \
        .withColumnRenamed("Foreign-born", "foreign_born") \
        .withColumnRenamed("Average Household Size", "avg_house_size") \
        .withColumnRenamed("State Code", "state_code") \
        .withColumnRenamed("Race", "race") \
        .withColumnRenamed("Count", "count") \
        .withColumn('city_state_code', F.concat_ws(', ', F.upper(F.col('city')), F.upper(F.col('state_code'))))

    data_output_full_file_path = f'{output_dir}/us-cities-demographics.parquet'
    us_cities_demographics_spark_df \
        .write \
        .options(encoding="ISO-8859-1") \
        .mode('overwrite') \
        .parquet(data_output_full_file_path)


def etl_i94_immigration(spark, input_dir, i94_immigration_data_files, output_dir):
    """Clean the i94 immigration data"""
    # load i94addr dictionary - map the i94addr values
    i94addr_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94addr_dictionary.csv'
    i94addr_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94addr_dictionary_input_full_file_path)

    # load i94cit dictionary - map the i94cit values
    i94cit_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94cit_dictionary.csv'
    i94cit_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94cit_dictionary_input_full_file_path)
    # i94cit_dictionary_spark_df - replace values in value cells of records whose value cells start with "INVALID: ", "No Country Code " and "should not show"
    i94cit_dictionary_spark_df = i94cit_dictionary_spark_df.withColumn('value',
                                                                       F.when(F.col('value').rlike('INVALID: ') | F.col('value').rlike('No Country Code ') | F.col(
                                                                           'value').rlike('should not show'), 'IN_i94cit_DICTIONARY_BUT_INVALID_UNKNOWN_NOTSHOW')
                                                                       .otherwise(F.col('value')))

    # load i94mode dictionary - map the i94mode values
    i94mode_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94mode_dictionary.csv'
    i94mode_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94mode_dictionary_input_full_file_path)

    # load i94port dictionary - map the i94port values
    i94port_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94port_dictionary.csv'
    i94port_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94port_dictionary_input_full_file_path)

    # # i94port_dictionary_spark_df - fitler out records whose value cells start with "No PORT Code "
    i94port_dictionary_spark_df = i94port_dictionary_spark_df.withColumn('value',
                                                                         F.when(F.col('value').rlike(
                                                                             'No PORT Code '), 'IN_i94cit_DICTIONARY_BUT_NO_PORT_CODE')
                                                                         .otherwise(F.col('value')))

    # load i94res dictionary - map the i94res values
    i94res_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94res_dictionary.csv'
    i94res_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94res_dictionary_input_full_file_path)

    # i94res_dictionary_spark_df - replace values in value cells of records whose value cells start with "INVALID: ", "No Country Code " and "should not show"
    i94res_dictionary_spark_df = i94res_dictionary_spark_df.withColumn('value',
                                                                       F.when(F.col('value').rlike('INVALID: ') | F.col('value').rlike('No Country Code ') | F.col(
                                                                           'value').rlike('should not show'), 'IN_i94res_DICTIONARY_BUT_INVALID_UNKNOWN_NOTSHOW')
                                                                       .otherwise(F.col('value')))

    # load i94visa dictionary - map the i94visa values
    i94visa_dictionary_input_full_file_path = f'{input_dir}/dictionary_data/i94visa_dictionary.csv'
    i94visa_dictionary_spark_df = spark \
        .read \
        .format('csv') \
        .options(header='true', inferSchema='true', encoding="ISO-8859-1") \
        .load(i94visa_dictionary_input_full_file_path)

    i94_immigration_data_list = []
    for i94_immigration_data_file in i94_immigration_data_files.split(','):
        # load i94 immigration data
        i94_immigration_input_full_file_path = f'{input_dir}/i94_immigration_2016_raw/{i94_immigration_data_file}'
        i94_immigration_spark_df = spark.read \
            .format('com.github.saurfang.sas.spark') \
            .load(i94_immigration_input_full_file_path,
                  forceLowercaseNames=True,
                  inferLong=True)

        # Cast some columns to the ideal type
        i94_immigration_spark_df = i94_immigration_spark_df \
            .withColumn('cicid',  F.col('cicid').cast('integer').cast('string')) \
            .withColumn('i94yr',  F.col('i94yr').cast('integer')) \
            .withColumn('i94mon', F.col('i94mon').cast('integer')) \
            .withColumn('i94cit', F.col('i94cit').cast('integer')) \
            .withColumn("i94res", F.col('i94res').cast('integer')) \
            .withColumn("i94mode", F.col('i94mode').cast('integer')) \
            .withColumn("i94bir", F.col('i94bir').cast('integer')) \
            .withColumn("i94visa", F.col('i94visa').cast('integer'))

        # process sas dates
        def sas_date_convert(days):
            date = datetime.strptime('1960-01-01', "%Y-%m-%d")
            try:
                return date + timedelta(days)
            except Exception as ex:
                return date

        def str_date_convert(str_date, fmt):
            try:
                date = datetime.strptime(str_date, fmt)
                return date
            except Exception as ex:
                return datetime.strptime('1960-01-01', "%Y-%m-%d")

        sas_date_convert_udf = F.udf(sas_date_convert, DateType())
        str_date_convert_udf = F.udf(str_date_convert, DateType())

        i94_immigration_spark_df = i94_immigration_spark_df \
            .withColumn('arrdate', sas_date_convert_udf(F.col('arrdate'))) \
            .withColumn('depdate', sas_date_convert_udf(F.col('depdate'))) \
            .withColumn('dtadfile', str_date_convert_udf(F.col('dtadfile'), F.lit('%Y%m%d'))) \
            .withColumn('dtaddto', str_date_convert_udf(F.col('dtaddto'), F.lit('%m%d%Y')))

        # map the i94addr values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94addr_dictionary_spark_df, i94_immigration_spark_df.i94addr == i94addr_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94addr') \
            .withColumnRenamed('value', 'i94addr')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94addr_DICTIONARY', ['i94addr'])

        # map the i94cit values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94cit_dictionary_spark_df, i94_immigration_spark_df.i94cit == i94cit_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94cit') \
            .withColumnRenamed('value', 'i94cit')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94cit_DICTIONARY', ['i94cit'])

        # map the i94mode values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94mode_dictionary_spark_df, i94_immigration_spark_df.i94mode == i94mode_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94mode') \
            .withColumnRenamed('value', 'i94mode')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94mode_DICTIONARY', ['i94mode'])

        # map the i94port values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94port_dictionary_spark_df, i94_immigration_spark_df.i94port == i94port_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94port') \
            .withColumnRenamed('value', 'i94port')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94port_DICTIONARY', ['i94port'])

        # map the i94res values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94res_dictionary_spark_df, i94_immigration_spark_df.i94res == i94res_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94res') \
            .withColumnRenamed('value', 'i94res')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94res_DICTIONARY', ['i94res'])

        # map the i94visa values
        i94_immigration_spark_df = i94_immigration_spark_df \
            .join(i94visa_dictionary_spark_df, i94_immigration_spark_df.i94visa == i94visa_dictionary_spark_df.key, 'left') \
            .drop('key', 'i94visa') \
            .withColumnRenamed('value', 'i94visa')

        i94_immigration_spark_df = i94_immigration_spark_df.fillna(
            'NOT_IN_i94visa_DICTIONARY', ['i94visa'])

        i94_immigration_spark_df = i94_immigration_spark_df.select('cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',
                                                                   'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate', 'i94bir', 'i94visa',
                                                                   'count', 'dtadfile', 'visapost', 'occup', 'entdepa', 'entdepd', 'entdepu', 'matflag',
                                                                   'biryear', 'dtaddto', 'gender', 'insnum', 'airline', 'admnum', 'fltno', 'visatype')

        i94_immigration_data_list.append(i94_immigration_spark_df)

    def union_all(*spark_dfs):
        return reduce(spark_DataFrame.union, spark_dfs)

    big_i94_immigration_spark_df = union_all(*i94_immigration_data_list)
    output_full_file_path = f'{output_dir}/i94_immigration_2016.parquet'
    big_i94_immigration_spark_df \
        .write \
        .options(encoding="ISO-8859-1") \
        .mode('overwrite') \
        .parquet(output_full_file_path)


def main():
    """main function of this script."""
    spark = create_spark_session()

    # get input dir and output dir
    input_data_dir = config['S3']['INPUT_S3_BUCKET']
    output_data_dir = config['S3']['OUTPUT_S3_BUCKET']

    # data cleaning
    etl_world_temperature(spark, input_data_dir, output_data_dir)
    etl_airport_code(spark, input_data_dir, output_data_dir)
    etl_us_cities_demographics(spark, input_data_dir, output_data_dir)

    i94_immigration_data_files = config['I94_IMMIGRATION']['I94_IMMIGRATION_FILES']
    etl_i94_immigration(spark, input_data_dir,
                        i94_immigration_data_files, output_data_dir)

    spark.stop()


if __name__ == '__main__':
    main()
