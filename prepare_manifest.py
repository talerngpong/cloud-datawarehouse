import json
from dataclasses import dataclass
from typing import List, Optional, Pattern

import boto3
import botocore

from common import get_static_config_instance, EtlConfig


@dataclass
class S3ManifestEntry:
    url: str
    mandatory: bool


@dataclass
class S3Manifest:
    entries: List[S3ManifestEntry]


def build_s3_manifest(object_summaries: List) -> S3Manifest:
    """
    Build `S3Manifest` object from S3 object summaries

        Parameters:
            object_summaries: list of object summary containing `bucket_name: str` and `key: str`

        Returns:
             s3_manifest: S3Manifest object
    """
    return S3Manifest(entries=[
        S3ManifestEntry(url=f's3://{object_summary.bucket_name}/{object_summary.key}', mandatory=True)
        for
        object_summary
        in
        object_summaries
    ])


def get_object_summaries(
        bucket,
        prefix: str,
        regex_pattern: Pattern[str]
) -> List:
    """
    Get S3 object summaries from S3 bucket with specific prefix and regular expression.

        Parameters:
            bucket: S3 bucket resource
            prefix: prefix used as S3 object filter
            regex_pattern: regular expression used as additional S3 object filter after using `prefix`

        Returns:
            object_summaries: list of object summaries
    """
    object_summaries = []

    for object_summary in bucket.objects.filter(Prefix=prefix):
        if regex_pattern.match(object_summary.key):
            object_summaries = [*object_summaries, object_summary]

    return object_summaries


def upload_object_summaries_as_manifest(
        s3,
        object_summaries: List,
        manifest_bucket_name: str,
        manifest_key: str
) -> None:
    """
    Upload Redshift manifest file created from S3 object summaries to particular S3 bucket

        Parameters:
            s3: S3 resource
            object_summaries: S3 object summaries
            manifest_bucket_name: S3 bucket name used to store Redshift manifest file
            manifest_key: S3 bucket key used to store Redshift manifest file

        Returns:
            None
    """
    s3_manifest: S3Manifest = build_s3_manifest(object_summaries)
    serialized_manifest = json.dumps({
        'entries': [
            {
                'url': entry.url,
                'mandatory': entry.mandatory
            }
            for entry
            in s3_manifest.entries
        ]
    })
    manifest_bytes = bytes(serialized_manifest, 'utf-8')
    obj = s3.Object(manifest_bucket_name, manifest_key)
    obj.put(Body=manifest_bytes)


def build_and_upload_manifest_file(
        s3,
        etl_config: EtlConfig
) -> None:
    """
    Build and upload manifest file to S3 bucket according to predefined ETL configuration

        Parameters:
            s3: S3 resource
            etl_config: ETL application configuration

        Returns:
            None
    """
    manifest_bucket_validation_error: Optional = None
    try:
        s3.meta.client.head_bucket(Bucket=etl_config.manifest.bucket_name)
    except botocore.exceptions.ClientError as e:
        manifest_bucket_validation_error = e

    if manifest_bucket_validation_error:
        error_code = manifest_bucket_validation_error.response['Error']['Code']

        # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/migrations3.html#accessing-a-bucket
        if error_code == '404':
            s3.create_bucket(
                Bucket=etl_config.manifest.bucket_name,
                CreateBucketConfiguration={
                    'LocationConstraint': etl_config.region_name
                }
            )
        else:
            raise manifest_bucket_validation_error

    dataset_bucket = s3.Bucket(etl_config.data_set.bucket_name)

    song_data_object_summaries = get_object_summaries(
        bucket=dataset_bucket,
        prefix=etl_config.data_set.song_data_prefix,
        regex_pattern=etl_config.data_set.song_data_regex_pattern
    )
    log_data_object_summaries = get_object_summaries(
        bucket=dataset_bucket,
        prefix=etl_config.data_set.log_data_prefix,
        regex_pattern=etl_config.data_set.log_data_regex_pattern
    )
    upload_object_summaries_as_manifest(
        s3,
        song_data_object_summaries,
        manifest_bucket_name=etl_config.manifest.bucket_name,
        manifest_key=etl_config.manifest.song_data_key
    )
    upload_object_summaries_as_manifest(
        s3,
        log_data_object_summaries,
        manifest_bucket_name=etl_config.manifest.bucket_name,
        manifest_key=etl_config.manifest.event_data_key
    )


def main() -> None:
    s3 = boto3.resource('s3')
    etl_config = get_static_config_instance()
    build_and_upload_manifest_file(s3, etl_config)


if __name__ == "__main__":
    main()
