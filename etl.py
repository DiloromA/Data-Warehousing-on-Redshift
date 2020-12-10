import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Inserts in bulk into staging tables and commits them.
    Args:
    cur -- SQL cursor object
    conn -- cluster connection string
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Inserts data into tables and commits them.
    Args:
    cur -- SQL cursor object
    conn -- cluster connection string
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Orchestrates the process of reading credentials from the configuration file, 
    establishing connection to the cluster, getting the cursor object, loading staging tables 
    and populating the fact and dimension tables with data from the staging tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()