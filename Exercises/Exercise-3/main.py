# import boto3
from botocore.config import Config
import gzip
import io
import requests
import warcio
import json
from urllib.parse import quote_plus
from warcio.archiveiterator import ArchiveIterator

# In this file there is no use of AWS S3 and boto package because I have no credentials 

# Constants
SERVER = 'http://index.commoncrawl.org/'
INDEX_NAME = 'CC-MAIN-2022-05'  # Using the index from the exercise
USER_AGENT = 'exercise-3-crawler/1.0 (Data Engineering Practice Exercise)'

def search_cc_index(url):
    """Search the Common Crawl index for a specific URL."""
    encoded_url = quote_plus(url)
    index_url = f'{SERVER}{INDEX_NAME}-index?url={encoded_url}&output=json'
    
    response = requests.get(
        index_url, 
        headers={'user-agent': USER_AGENT}
    )
    
    if response.status_code == 200:
        records = response.text.strip().split('\n')
        return [json.loads(record) for record in records]
    return None

def fetch_warc_content(record):
    """Fetch and process WARC content from Common Crawl."""
    offset, length = int(record['offset']), int(record['length'])
    warc_url = f'https://data.commoncrawl.org/{record["filename"]}'
    
    # Define byte range for the request
    byte_range = f'bytes={offset}-{offset+length-1}'
    
    response = requests.get(
        warc_url,
        headers={
            'user-agent': USER_AGENT,
            'Range': byte_range
        },
        stream=True
    )
    
    if response.status_code == 206:  # Successful partial content response
        stream = ArchiveIterator(response.raw)
        for warc_record in stream:
            if warc_record.rec_type == 'response':
                # Stream and print the content
                content = warc_record.content_stream().read()
                print(content.decode('utf-8'))
                return True
    return False

def main():
    try:
        # Example URL to search for (you can modify this)
        target_url = 'commoncrawl.org'
        
        print(f"Searching Common Crawl index for: {target_url}")
        records = search_cc_index(target_url)
        
        if records:
            print(f"Found {len(records)} records")
            
            # Process the first record
            first_record = records[0]
            print(f"Processing record from: {first_record['filename']}")
            
            success = fetch_warc_content(first_record)
            if not success:
                print("Failed to process WARC content")
        else:
            print("No records found")
            
    except requests.RequestException as e:
        print(f"Network error occurred: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
