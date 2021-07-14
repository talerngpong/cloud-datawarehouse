# ETL on Cloud Data Warehouse for Song Play Analysis

This project aims to transform raw song and user data and load them into Redshift cluster for later analysis. This is also used to satisfied with `Data Warehouse` project under [Data Engineer Nanodegree Program](https://www.udacity.com/course/data-engineer-nanodegree--nd027).

## Prerequisite
- Python3
- Python virtual environment (aka `venv`)
- AWS credentials/config files under `~/.aws` directories (see more: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html)

## Steps
1. Bootstrap virtual environment with dependencies
   ```bash
   $ python3 -m venv ./venv
   $ source ./venv/bin/activate
   $ pip install -r requirements.txt
   ```
2. Copy config template `template.dwh.cfg` to `dwh.cfg`.
   ```bash
   $ cp ./template.dwh.cfg ./dwh.cfg
   ```
3. Fill `dwh.cfg` on `CLUSTER` and `MANIFEST` sections
   - For `CLUSTER` section, this will be used to construct Redshift cluster from scratch. We are free to choose our values. Here are possible values.
   ```cfg
   [CLUSTER]
   DB_NAME=dwh
   DB_USER=dwhuser
   DB_PASSWORD=<choose_whatever_you_want>
   DB_PORT=5439
   CLUSTER_TYPE=multi-node
   NUM_NODES=4
   NODE_TYPE=dc2.large
   CLUSTER_IDENTIFIER=dwhCluster
   IAM_ROLE_NAME=dwhRole
   ```
   - For `MANIFEST` section, this refers to another S3 bucket storing Redshift manifest files that we will create later. Here are possible values.
   ```cfg
   [MANIFEST]
   BUCKET_NAME=sample-bucket-for-udacity-data-warehouse-project
   EVENT_DATA_KEY=sample-path/sample-log-data-manifest.json
   SONG_DATA_KEY=sample-path/sample-song-data-manifest.json
   ```
4. Prepare manifest files.
   ```bash
   $ python prepare_manifest.py
   ```
5. Spin up Redshift cluster.
   ```bash
   $ python spin_dwh_up.py
   ```
6. Create tables and do ETL.
   ```bash
   $ python create_tables.py
   $ python etl.py
   ```
7. When finished using Redshift cluster, tear it down.
   ```bash
   $ python tear_dwh_down.py
   ```
