import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import time
import sys

def load_staging_tables(cur, conn):
    """This function loads raw data from S3 to the correspoding staging tables in the copy_table_queries list.
    """
    for query in copy_table_queries:
        try:
            starter = time.time()
            cur.execute(query)
            conn.commit()
            print(f'Done load stage table - {time.time() - starter}s:\n\t{query}')
        except Exception as ex:
            print(f'load_staging_tables failed for: {ex}')
            print(f'{query}')
            sys.exit(-1)
            

def insert_tables(cur, conn):
    """This function inserts records from staging tables to the tables in the insert_table_queries list.
    """
    for query in insert_table_queries:
        starter = time.time()

        try:
            cur.execute(query)
            conn.commit()
            print(f'Done insert table - {time.time() - starter}s:\n{query}')
        except Exception as ex:
            print(f'{ex}')
            print(f'insert_tables failed for {query}')
            sys.exit(-1)


def main():
    """The main function of this etl script, it will execute relevant functions to load and insert records into tables in copy_table_queries and insert_table_queries lists.
    """
    starter = time.time()
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # create denano_proj3 as the schema
    cur.execute("""SET search_path TO dwh;
    """)
    conn.commit()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()

    print(f'Done etl.py - {time.time() - starter}s')


if __name__ == "__main__":
    main()