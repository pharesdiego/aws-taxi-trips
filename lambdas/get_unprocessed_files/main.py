import boto3
import json
from typing import TypedDict

s3 = boto3.client('s3')


class ExpectedEvent(TypedDict):
    source_bucket: str
    target_bucket: str


def get_keys_in_bucket(bucket: str) -> list[str]:
    iterator = s3.get_paginator('list_objects_v2').paginate(Bucket=bucket)

    try:
        file_keys = [content['Key']
                     for page in iterator for content in page['Contents']]

        return file_keys
    except:
        return []


def get_partition_path_from_key(file_key: str) -> str:
    return '/'.join(file_key.split('/')[:-1])


def handler(event: ExpectedEvent = {}, context={}):
    """
    Returns a list of unprocessed files by comparing which partitions
    exist in source_bucket but don't exist in target_bucket
    """
    print('Working on: ', json.dumps(event))

    source_bucket_keys = get_keys_in_bucket(event['source_bucket'])
    target_bucket_keys = '~'.join(get_keys_in_bucket(event['target_bucket']))

    result = list(filter(lambda s_key: get_partition_path_from_key(
        s_key) not in target_bucket_keys, source_bucket_keys))

    print('Result: ', json.dumps(result))

    return result
