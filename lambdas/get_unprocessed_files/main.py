import boto3
import json
from typing import TypedDict

s3 = boto3.client('s3')


class ExpectedEvent(TypedDict):
    bucket_name: str


def get_keys_in_bucket(bucket: str) -> list[str]:
    iterator = s3.get_paginator('list_objects_v2').paginate(Bucket=bucket)

    try:
        file_keys = [content['Key']
                     for page in iterator for content in page['Contents']]

        return file_keys
    except:
        return []


def is_key_marked_as_gzipped(bucket: str, key: str) -> bool:
    return 'gzipped' in s3.head_object(Bucket=bucket, Key=key)['Metadata']


def get_partition_path_from_key(file_key: str) -> str:
    return '/'.join(file_key.split('/')[:-1])


def handler(event: ExpectedEvent = {}, context={}):
    """
    Returns a list of unprocessed files in a specified bucket.
    An unprocessed file is one without a 'gzipped' metadata key.
    """
    print('Working on: ', json.dumps(event))

    keys_in_bucket = get_keys_in_bucket(event['bucket_name'])

    return list(filter(lambda key: not is_key_marked_as_gzipped(event['bucket_name'], key), keys_in_bucket))
