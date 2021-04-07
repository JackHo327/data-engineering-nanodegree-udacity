import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import time
import sys

def drop_tables(cur, conn):
    """This function drops tables specifed in the drop_table_queries lists.
    """
    for query in drop_table_queries:
        starter = time.time()
        cur.execute(query)
        conn.commit()
        print(f'Done drop table - {time.time() - starter}s:\n\t{query}')


def create_tables(cur, conn):
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
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # create denano_proj3 as the schema
    cur.execute("""CREATE SCHEMA IF NOT EXISTS dwh;
SET search_path TO dwh;
    """)
    conn.commit()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    
    print(f'Done create_tables.py - {time.time() - starter}s')

if __name__ == "__main__":
    main()