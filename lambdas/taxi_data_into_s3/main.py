from bs4 import BeautifulSoup
import re
import requests
import boto3
import os
import io

s3_client = boto3.client('s3')
bucket_name = os.environ['RAW_TAXI_DATA_BUCKET_NAME']
file_tracker_key = 'file_tracker.txt'
one_mb_in_bytes = 1000000

# Remember to execute script from same folder. This is a relative path
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

def get_date_from_url(url):
    match = re.search('\w{4}-\d\d', url)

    return match[0] if match else None
    
def handler():
    html = requests.get('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page').text
    soup = BeautifulSoup(html, features='html5lib')
    anchors = soup.find_all('a', {'href': re.compile(r'.*yellow_tripdata_202.*')})
    extracted_anchors_urls = [anchor['href'] for anchor in anchors]
    already_extracted_files = get_already_extracted_files()

    files_url_to_be_downloaded = list(filter(lambda url: get_date_from_url(url) not in already_extracted_files, extracted_anchors_urls))

    for file_url in files_url_to_be_downloaded:
        response = requests.get(file_url, stream=True)
        with open(f'data/{get_date_from_url(file_url)}.parquet', 'wb') as file:
            for chunk in response.iter_content(chunk_size=one_mb_in_bytes):
                file.write(chunk)
        
        with open('__already_extracted_files.txt', 'a') as tracker_file:
            tracker_file.write(get_date_from_url(file_url) + '\n')


handler()