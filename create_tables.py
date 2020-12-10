import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import configparser


def drop_tables(cur, conn):
    """Drops existing tables and commits them.
    Args:
    cur -- SQL cursor object
    conn -- cluster connection string
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates tables and commits them.
    Args:
    cur -- SQL cursor object
    conn -- cluster connection string
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Orchestrates the process of reading credentials from the configuration file, 
    establishing connection to the cluster, getting the cursor object and executing drop tables and create
    tables commands.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()