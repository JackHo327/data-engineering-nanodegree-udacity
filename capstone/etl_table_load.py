# This script will load data from S3 into relevant tables in redshift - i94_immigration, airport, us_demographics and us_temperature
import pandas as pd
import json
import psycopg2
import configparser
import time
import sys
import os

config = configparser.ConfigParser()
config.read('capstone.cfg')


# compose copy statements
load_us_temperature_table = """copy us_temperature from '{_s3_}/USCitiesTemperaturesByMonth.parquet/' iam_role '{_iam_role_}' COMPUPDATE OFF STATUPDATE OFF FORMAT AS PARQUET;
""".format(_s3_=config['S3']['CLEAN_DATA_S3_BUCKET'], 
           _iam_role_=config['REDSHIFT']['IAM_ROLE'])
            
load_us_demographics_table = """copy us_demographics from '{_s3_}/us-cities-demographics.parquet/' iam_role '{_iam_role_}' COMPUPDATE OFF STATUPDATE OFF FORMAT AS PARQUET;
""".format(_s3_=config['S3']['CLEAN_DATA_S3_BUCKET'], 
           _iam_role_=config['REDSHIFT']['IAM_ROLE'])
            
load_airport_table = """copy airport from '{_s3_}/airport-codes.parquet/' iam_role '{_iam_role_}' COMPUPDATE OFF STATUPDATE OFF FORMAT AS PARQUET;
""".format(_s3_=config['S3']['CLEAN_DATA_S3_BUCKET'], 
           _iam_role_=config['REDSHIFT']['IAM_ROLE'])
            
load_i94_immigration_table = """copy i94_immigration from '{_s3_}/i94_immigration_2016.parquet/' iam_role '{_iam_role_}' COMPUPDATE OFF STATUPDATE OFF FORMAT AS PARQUET;
""".format(_s3_=config['S3']['CLEAN_DATA_S3_BUCKET'], 
           _iam_role_=config['REDSHIFT']['IAM_ROLE'])

def load_tables(cur, conn, load_table_queries):
    """This function loads tables specifed in the load_table_queries lists.
    """
    for query in load_table_queries:
        starter = time.time()

        try:
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f'{ex}')
            print(f'load_tables failed for {query}')
            sys.exit(-1)
            
        print(f'Done load table - {time.time() - starter}s:\n{query}')

def main():
    """The main function of this load_tables script, it will execute relevant functions to load relevant tables in the drop_table_queries and load_tables_queries lists.
    """
    starter = time.time()
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    
    cur.execute(f"""SET search_path TO {config['REDSHIFT']['SCHEMA']};""")
    conn.commit()
    
    load_tables(cur, conn, [load_us_temperature_table, load_us_demographics_table, load_airport_table, load_i94_immigration_table])

    conn.close()
    
    print(f'Done etl_table_load.py - {time.time() - starter}s')

if __name__ == "__main__":
    main()
