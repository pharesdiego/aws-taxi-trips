import boto3
import pandas as pd
from typing import TypedDict
from io import BytesIO
import json


class ExpectedEvent(TypedDict):
    file_key: str
    bucket_name: str


def handler(event: ExpectedEvent, context):
    """
    Convert and overwrites file in S3 from ZSTD compression to GZIP.
    """
    print('Working on: ', json.dumps(event))

    s3 = boto3.client('s3')

    response = s3.get_object(
        Bucket=event['bucket_name'], Key=event['file_key'])

    zstd_compressed_data = response['Body'].read()

    temp_file = '/tmp/data.parquet'

    with open(temp_file, 'wb+') as tf:
        pd.read_parquet(BytesIO(zstd_compressed_data)).to_parquet(
            tf, compression='gzip', engine='fastparquet')

    s3.upload_file(Bucket=event['bucket_name'], Key=event['file_key'],
                   Filename=temp_file, ExtraArgs={'Metadata': {'gzipped': 'ᕦ(ò_óˇ)ᕤ'}})
