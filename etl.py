import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This Function Loads data from Amazon S3 into Stage Tables"""
    for query in copy_table_queries:
        print("Table staging----",query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This Function Inserts Data into Fact and Dimentions """
    for query in insert_table_queries:
        print("Table Insert----",query)
        cur.execute(query)
        conn.commit()


def main():
        """This is the Main Function which establishes connection to Redshift ,Loads data from Amazon S3 into Stage Tables and Inserts Data into Fact and Dimentions"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()