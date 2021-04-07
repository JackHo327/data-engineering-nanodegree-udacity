# This script will create relevant tables in redfshift - i94_immigration, airport, us_demographics and us_temperature
import pandas as pd
import json
import psycopg2
import configparser
import time
import sys

config = configparser.ConfigParser()
config.read('capstone.cfg')

# compose create statements
create_i94_immigration_table = """
    DROP TABLE IF EXISTS i94_immigration;
    CREATE TABLE i94_immigration (
        cicid varchar(256) primary key,
        i94yr integer,
        i94mon integer,
        i94cit varchar(256),
        i94res varchar(256),
        i94port varchar(256) distkey,
        arrdate date,
        i94mode varchar(256),
        i94addr varchar(256),
        depdate date,
        i94bir integer,
        i94visa varchar(256),
        count integer,
        dtadfile date,
        visapost varchar(256),
        occup varchar(256),
        entdepa varchar(256),
        entdepd varchar(256),
        entdepu varchar(256),
        matflag varchar(256),
        biryear double precision,
        dtaddto date,
        gender varchar(25),
        insnum varchar(256),
        airline varchar(256),
        admnum double precision,
        fltno varchar(256),
        visatype varchar(256)
    )
    sortkey(i94mon, i94visa, arrdate, depdate, dtadfile, dtaddto, i94cit, i94res, i94mode);
"""


create_us_temperature_table = """
    DROP TABLE IF EXISTS us_temperature;
    CREATE TABLE us_temperature (
        month integer,
        day integer,
        city varchar(256),
        state varchar(256),
        state_code varchar(256),
        city_state_code varchar(256) distkey,
        avg_temperature double precision,
        primary key (city_state_code, month)
    )
    sortkey(month, avg_temperature);
"""


create_us_demographics_table = """
    DROP TABLE IF EXISTS us_demographics;
    CREATE TABLE us_demographics(
        city varchar(256),
        state varchar(256),
        median_age double precision,
        male_population integer,
        female_population integer,
        total_population integer,
        num_of_veterans integer,
        foreign_born integer,
        avg_house_size double precision,
        state_code varchar(256),
        race varchar(256),
        count integer,
        city_state_code varchar(256) distkey,
        primary key (city_state_code, race)
    )
    sortkey(foreign_born, total_population, avg_house_size, median_age);
"""


create_airport_table = """
    DROP TABLE IF EXISTS airport;
    CREATE TABLE airport (
        ident varchar(256),
        type varchar(256),
        airport_name varchar(256),
        elevation_ft integer,
        continent varchar(256),
        iso_country varchar(256),
        municipality varchar(256),
        gps_code varchar(256),
        iata_code varchar(256),
        local_code varchar(256),
        region varchar(256),
        municipality_region varchar(256) distkey,
        primary key (municipality_region, airport_name)
    )
    sortkey(type, elevation_ft);
"""

def create_tables(cur, conn, create_table_queries):
    """This function creates tables specifed in the create_table_queries lists.
    """
    for query in create_table_queries:
        starter = time.time()

        try:
            cur.execute(query)
            conn.commit()
        except Exception as ex:
            print(f'{ex}')
            print(f'create_tables failed for {query}')
            sys.exit(-1)
            
        print(f'Done create table - {time.time() - starter}s:\n{query}')

def main():
    """The main function of this create_tables script, it will execute relevant functions to drop and create relevant tables in the drop_table_queries and create_table_queries lists.
    """
    starter = time.time()
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['REDSHIFT'].values()))
    cur = conn.cursor()
    
    cur.execute(f"""CREATE SCHEMA IF NOT EXISTS {config['REDSHIFT']['SCHEMA']};
                SET search_path TO {config['REDSHIFT']['SCHEMA']};""")
    conn.commit()
    
    create_tables(cur, conn, [create_i94_immigration_table, create_us_temperature_table, create_us_demographics_table, create_airport_table])

    conn.close()
    
    print(f'Done etl_table_create.py - {time.time() - starter}s')

if __name__ == "__main__":
    main()
