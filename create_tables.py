import boto3
import psycopg2

from common import get_cluster_endpoint, get_static_config_instance, EtlConfig
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn) -> None:
    """
    Drop tables defined in `drop_table_queries`.

        Parameters:
            cur: active DB cursor
            conn: active DB connection

        Returns:
            None
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f'There is an exception when executing query = {query}')
            raise e


def create_tables(cur, conn) -> None:
    """
        Create tables defined in `create_table_queries`.

            Parameters:
                cur: active DB cursor
                conn: active DB connection

            Returns:
                None
        """
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except Exception as e:
            print(f'There is an exception when executing query = {query}')
            raise e


def main():
    etl_config: EtlConfig = get_static_config_instance()

    redshift_client = boto3.client('redshift')

    cluster_endpoint = get_cluster_endpoint(redshift_client, etl_config=etl_config)

    # https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
    conn_string = 'host={endpoint} dbname={db_name} user={db_user} password={db_password} port={db_port}'.format(
        db_user=etl_config.redshift_cluster.db_user,
        db_password=etl_config.redshift_cluster.db_password,
        endpoint=cluster_endpoint,
        db_port=etl_config.redshift_cluster.db_port,
        db_name=etl_config.redshift_cluster.db_name
    )
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
