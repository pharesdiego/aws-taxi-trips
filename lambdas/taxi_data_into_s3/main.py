from bs4 import BeautifulSoup
import re
import requests
import boto3
import os
import json

s3_client = boto3.client('s3')
bucket_name = os.environ['RAW_TAXI_DATA_BUCKET_NAME']
taxi_page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'


def get_already_extracted_files() -> str:
    """
    Gets a list with S3 file keys, then returns them as yyyy-mm to match the format used in the parquet's download url
    """
    iterator = s3_client.get_paginator(
        'list_objects_v2').paginate(Bucket=bucket_name)

    try:
        # year=yyyy/month=mm/data.parquet
        file_keys = [content['Key']
                     for page in iterator for content in page['Contents']]

        return [f'{key[5:9]}-{key[16:18]}' for key in file_keys]
    except Exception as e:
        print('Error while creating file keys list', e)
        return []


def get_date_from_file_url(url: str) -> str:
    """
    Extract the date in yyyy-mm format from the file's url

    :raises Exception: If match is not found. This is because the date is important to create the folder partition
    """
    match = re.search('\w{4}-\d\d', url)

    if (match):
        return match[0]
    else:
        raise Exception(f"Couldn't extract date from {url}")


def get_s3_partitioned_key_path(yyyy_mm_date: str) -> str:
    year, month = yyyy_mm_date.split('-')

    return f'year={year}/month={month}/data.parquet'


def get_files_url_to_be_extracted(already_extracted_files: str) -> list:
    """
    Returns a list of urls extracted from <a/>'s in the page,
    excluding those urls already extracted
    """

    html = requests.get(taxi_page_url).text
    soup = BeautifulSoup(html, features='lxml')

    anchors = soup.find_all(
        'a', {'href': re.compile(r'.*yellow_tripdata_2023.*')})

    extracted_urls_from_anchors = [anchor['href'] for anchor in anchors]

    return list(
        filter(
            lambda url: get_date_from_file_url(url) not in already_extracted_files, extracted_urls_from_anchors)
    )


def is_it_allowed_and_thus_legal_to_mine_this_data(response: requests.Response):
    return response.ok


def handler(event, context):
    already_extracted_files = get_already_extracted_files()
    files_urls_to_be_extracted = get_files_url_to_be_extracted(
        already_extracted_files)
    extracting_results = {'successes': [], 'failed': []}

    for file_url in files_urls_to_be_extracted:
        response = requests.get(file_url)
        file_date = get_date_from_file_url(file_url)
        file_s3_key = get_s3_partitioned_key_path(file_date)

        if (is_it_allowed_and_thus_legal_to_mine_this_data(response)):
            try:
                s3_client.put_object(
                    Body=response.content,
                    Bucket=bucket_name,
                    Key=file_s3_key
                )

                extracting_results['successes'].append(file_s3_key)
            except Exception as e:
                print(f"Error while loading {file_s3_key} to S3: {e}")
                extracting_results['failed'].append(file_s3_key)
        else:
            print(f"Response's not okay for: {file_s3_key}")
            extracting_results['failed'].append(file_s3_key)

    print('Files expected to be extracted: ', files_urls_to_be_extracted)
    print('Execution result: ', json.dumps(extracting_results))
    return extracting_results
