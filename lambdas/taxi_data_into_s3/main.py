from bs4 import BeautifulSoup
import re
import requests
import boto3
import os
import io

s3_client = boto3.client('s3')
bucket_name = os.environ['RAW_TAXI_DATA_BUCKET_NAME']
file_tracker_key = 'file_tracker.txt'
taxi_page_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'


def get_already_extracted_files():
    """Returns file_tracker content or creates a new one if tracker doesn't exists"""
    try:
        response = s3_client.get_object(
            Bucket=bucket_name,
            Key=file_tracker_key
        )

        return response['Body'].read().decode('utf-8')
    except:
        temp_file = io.StringIO('')

        s3_client.put_object(
            Body=temp_file.getvalue(),
            Bucket=bucket_name,
            Key=file_tracker_key
        )

        temp_file.close()

        return ''


def get_file_date_from_url(url):
    match = re.search('\w{4}-\d\d', url)

    return match[0] if match else None


def get_s3_path_for_data(date):
    year, month = date.split('-')

    return f'year={year}/month={month}/data.parquet'


def get_files_url_to_be_extracted(already_extracted_files):
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
            lambda url: get_file_date_from_url(url) not in already_extracted_files, extracted_urls_from_anchors)
    )


def handler(event, context):
    already_extracted_files = get_already_extracted_files()
    files_urls_to_be_extracted = get_files_url_to_be_extracted(
        already_extracted_files)
    extracted_files = []

    for file_url in files_urls_to_be_extracted:
        response = requests.get(file_url)

        try:
            file_date = get_file_date_from_url(file_url)
            file_s3_key = get_s3_path_for_data(file_date)

            s3_client.put_object(
                Body=response.content,
                Bucket=bucket_name,
                Key=file_s3_key
            )

            extracted_files.append(file_date)
        except Exception as e:
            raise Exception(
                f'Error while trying to upload {file_url} to {bucket_name} at {file_s3_key}') from e

    if len(extracted_files):
        extracted_files_as_lines = map(lambda file_date: file_date +
                                       '\n', extracted_files)

        try:
            s3_client.put_object(
                Body=already_extracted_files +
                ''.join(extracted_files_as_lines),
                Bucket=bucket_name,
                Key=file_tracker_key
            )
        except Exception as e:
            raise Exception(
                f'Error while appending {", ".join(extracted_files)} to tracker file') from e

    return extracted_files
