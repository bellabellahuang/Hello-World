import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function is used to drop the tables if they exist.
        
        INPUTS: 
        * cur - the cursor variable
        * conn - the connection variable
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
        This function is used to create the tables.
        
        INPUTS: 
        * cur - the cursor variable
        * conn - the connection variable
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        The main function to run when this script is executed.
    """
    # read the aws configs from the config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connect to the redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # clear existed tables
    drop_tables(cur, conn)
    # create tables
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()