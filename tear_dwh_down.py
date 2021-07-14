import time

import boto3

from common import get_static_config_instance, EtlConfig


def poll_until_cluster_not_found(redshift_client, cluster_identifier: str) -> None:
    try:
        cluster_props = redshift_client.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        print(
            f"Cluster status (of identifier {cluster_identifier}) = {cluster_props['ClusterStatus']}. " +
            "Let's wait for 5 seconds."
        )
        time.sleep(5)
        poll_until_cluster_not_found(redshift_client, cluster_identifier)
    except redshift_client.exceptions.ClusterNotFoundFault as e:
        print(e)
        print(f'Cluster (of identifier {cluster_identifier}) is gone.')


def main() -> None:
    etl_config: EtlConfig = get_static_config_instance()

    redshift_client = boto3.client('redshift')

    redshift_client.delete_cluster(
        ClusterIdentifier=etl_config.redshift_cluster.cluster_identifier,
        SkipFinalClusterSnapshot=True
    )
    start_epoch = time.time()
    poll_until_cluster_not_found(
        redshift_client,
        cluster_identifier=etl_config.redshift_cluster.cluster_identifier
    )
    end_epoch = time.time()
    print(
        f'Successfully delete cluster with identifier = {etl_config.redshift_cluster.cluster_identifier} ' +
        f'in {int(end_epoch - start_epoch)} seconds'
    )


if __name__ == '__main__':
    main()
