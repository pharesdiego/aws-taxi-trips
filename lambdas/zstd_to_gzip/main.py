import boto3
import pandas as pd
from typing import TypedDict
from io import BytesIO
import json

s3 = boto3.client('s3')


def is_already_gzipped(bucket: str, key: str):
    return 'gzipped' in s3.head_object(Bucket=bucket, Key=key)['Metadata']


def convert_file_to_gzip(bucket: str, key: str):
    """
    Using pandas to convert to GZIP. Pandas is used because it is a
    convenient wrapper around cramjam.
    """
    response = s3.get_object(
        Bucket=bucket, Key=key)

    zstd_compressed_data = response['Body'].read()

    temp_file = '/tmp/data.parquet'

    with open(temp_file, 'wb+') as tf:
        pd.read_parquet(BytesIO(zstd_compressed_data)).to_parquet(
            tf, compression='gzip', engine='fastparquet')

    s3.upload_file(Bucket=bucket, Key=key,
                   Filename=temp_file, ExtraArgs={'Metadata': {'gzipped': 'ᕦ(ò_óˇ)ᕤ'}})


class ExpectedEvent(TypedDict):
    file_key: str
    bucket_name: str


def handler(event: ExpectedEvent, context):
    """
    Convert and overwrites file in S3 from ZSTD compression to GZIP.
    """
    print('Working on: ', json.dumps(event))
    bucket_name = event['bucket_name']
    file_key = event['file_key']

    if not is_already_gzipped(bucket_name, file_key):
        convert_file_to_gzip(bucket_name, file_key)
        print('Successfully gzipped: ', file_key)
    else:
        print('Already gzipped: ', file_key)
