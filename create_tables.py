import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Executes all DROP TABLE commands
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes all CREATE TABLE commands
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads configuration from dwh.cfg file and connects to the database
    Drops and creates all tables by calling drop_tables and create_tables
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}"
        .format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
