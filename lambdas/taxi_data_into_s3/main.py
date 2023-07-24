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
one_mb_in_bytes = 1000000

def get_already_extracted_files():
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

# Date's format: yyyy-mm
def get_s3_path_for_data(date):
    return '/'.join(date.split('-')) + '/data.parquet'

def get_files_url_to_be_extracted(already_extracted_files):
    html = requests.get(taxi_page_url).text
    soup = BeautifulSoup(html, features='html5lib')

    anchors = soup.find_all('a', {'href': re.compile(r'.*yellow_tripdata_2023.*')})

    extracted_urls_from_anchors = [anchor['href'] for anchor in anchors]

    return list(filter(lambda url: get_file_date_from_url(url) not in already_extracted_files, extracted_urls_from_anchors))
    
def handler(event, context):
    already_extracted_files = get_already_extracted_files()
    files_urls_to_be_extracted = get_files_url_to_be_extracted(already_extracted_files)

    new_extracted_files = []

    for file_url in files_urls_to_be_extracted:
        file_date = get_file_date_from_url(file_url)
        
        print(f'downloading: {file_url}')

        response = requests.get(file_url)

        try:
            s3_client.put_object(
                Body=response.content,
                Bucket=bucket_name,
                Key=get_s3_path_for_data(file_date)
            )

            new_extracted_files.append(file_date)
        except:
            print(f'error while uploading {file_url} to s3')
    
    try:
        s3_client.put_object(
            Body=map(lambda file_date: file_date + '\n', new_extracted_files)
        )
    except:
        print('error while writing to tracker file')
