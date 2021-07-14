import time
from typing import List

import boto3
import psycopg2

from common import get_cluster_endpoint, get_iam_role_arn, get_static_config_instance, EtlConfig
from sql_queries import build_copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn, etl_config: EtlConfig, dwh_iam_role_arn: str) -> None:
    """
    Load raw data to staging tables in Redshift cluster

        Parameters:
            cur: active DB cursor
            conn: active DB connection
            etl_config: ETL application configuration
            dwh_iam_role_arn: IAM role ARN used for Redshift data warehouse

        Returns:
            None
    """
    copy_table_queries: List[str] = build_copy_table_queries(etl_config=etl_config, dwh_iam_role_arn=dwh_iam_role_arn)
    for query in copy_table_queries:
        try:
            print(f'Start executing query = {query}')
            start_epoch = time.time()
            cur.execute(query)
            conn.commit()
            end_epoch = time.time()
            print(f'Spent {int(end_epoch - start_epoch)} seconds on executing query = {query}')
        except Exception as e:
            print(f'There is an exception when executing query = {query}')
            raise e


def insert_tables(cur, conn) -> None:
    """
    Insert data to fact and dimensional tables according to `insert_table_queries`

       Parameters:
           cur: active DB cursor
           conn: active DB connection

       Returns:
           None
    """
    for query in insert_table_queries:
        try:
            print(f'Start executing query = {query}')
            start_epoch = time.time()
            cur.execute(query)
            conn.commit()
            end_epoch = time.time()
            print(f'Spent {int(end_epoch - start_epoch)} seconds on executing query = {query}')
        except Exception as e:
            print(f'There is an exception when executing query = {query}')
            raise e


def main() -> None:
    etl_config: EtlConfig = get_static_config_instance()

    iam_client = boto3.client('iam')
    redshift_client = boto3.client('redshift')

    dwh_iam_role_arn = get_iam_role_arn(iam_client, iam_role_name=etl_config.redshift_cluster.iam_role_name)
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

    load_staging_tables(cur, conn, etl_config=etl_config, dwh_iam_role_arn=dwh_iam_role_arn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
