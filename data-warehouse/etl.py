import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function is used to load data from S3 into redshift staging tables.
        
        INPUTS: 
        * cur - the cursor variable
        * conn - the connection variable
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
        This function is used to insert data into the fact and dimension tables.
        
        INPUTS: 
        * cur - the cursor variable
        * conn - the connection variable
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        The main function to run when this script is executed.
    """
    # read the configs from the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # load data into staging tables
    load_staging_tables(cur, conn)
    # insert data into fact and dimension tables
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()