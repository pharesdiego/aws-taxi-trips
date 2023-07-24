from bs4 import BeautifulSoup
import re
import requests

one_mb_in_bytes = 1000000

# Remember to execute script from same folder. This is a relative path
def get_already_extracted_files():
    with open('__already_extracted_files.txt', 'r') as file:
        return file.read()

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