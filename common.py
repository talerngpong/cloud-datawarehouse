import configparser
from configparser import ConfigParser
from dataclasses import dataclass
import re
import boto3
from typing import Pattern


@dataclass
class Manifest:
    bucket_name: str
    event_data_key: str
    song_data_key: str


@dataclass
class DataSet:
    bucket_name: str
    song_data_prefix: str
    song_data_regex_pattern: Pattern[str]
    log_data_prefix: str
    log_data_regex_pattern: Pattern[str]
    log_data_json_path_key: str


@dataclass
class RedshiftCluster:
    db_name: str
    db_user: str
    db_password: str
    db_port: int
    cluster_type: str
    num_nodes: int
    node_type: str
    cluster_identifier: str
    iam_role_name: str


@dataclass
class EtlConfig:
    region_name: str
    manifest: Manifest
    data_set: DataSet
    redshift_cluster: RedshiftCluster


def get_static_config_instance() -> EtlConfig:
    region_name = boto3.session.Session().region_name

    config: ConfigParser = configparser.ConfigParser()
    config.read('dwh.cfg')

    manifest = Manifest(
        bucket_name=config['MANIFEST'].get('BUCKET_NAME'),
        event_data_key=config['MANIFEST'].get('EVENT_DATA_KEY'),
        song_data_key=config['MANIFEST'].get('SONG_DATA_KEY')
    )
    data_set = DataSet(
        bucket_name=config['DATA_SET'].get('BUCKET_NAME'),
        song_data_prefix=config['DATA_SET'].get('SONG_DATA_PREFIX'),
        song_data_regex_pattern=re.compile(config['DATA_SET'].get('SONG_DATA_REGEX_PATTERN')),
        log_data_prefix=config['DATA_SET'].get('LOG_DATA_PREFIX'),
        log_data_regex_pattern=re.compile(config['DATA_SET'].get('LOG_DATA_REGEX_PATTERN')),
        log_data_json_path_key=config['DATA_SET'].get('LOG_DATA_JSON_PATH_KEY')
    )
    redshift_cluster = RedshiftCluster(
        db_name=config['CLUSTER'].get('DB_NAME'),
        db_user=config['CLUSTER'].get('DB_USER'),
        db_password=config['CLUSTER'].get('DB_PASSWORD'),
        db_port=config['CLUSTER'].getint('DB_PORT'),
        cluster_type=config['CLUSTER'].get('CLUSTER_TYPE'),
        num_nodes=config['CLUSTER'].getint('NUM_NODES'),
        node_type=config['CLUSTER'].get('NODE_TYPE'),
        cluster_identifier=config['CLUSTER'].get('CLUSTER_IDENTIFIER'),
        iam_role_name=config['CLUSTER'].get('IAM_ROLE_NAME')
    )

    return EtlConfig(
        region_name=region_name,
        manifest=manifest,
        data_set=data_set,
        redshift_cluster=redshift_cluster
    )


def get_cluster_endpoint(redshift_client, etl_config: EtlConfig) -> str:
    cluster_props = redshift_client.describe_clusters(
        ClusterIdentifier=etl_config.redshift_cluster.cluster_identifier
    )['Clusters'][0]

    return cluster_props['Endpoint']['Address']


def get_iam_role_arn(iam_client, iam_role_name: str) -> str:
    return iam_client.get_role(RoleName=iam_role_name)['Role']['Arn']
