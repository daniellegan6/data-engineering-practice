import requests
import os
from urllib.parse import urlparse
import zipfile
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def is_valid_url(url):
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

def download_file(url, downloads_dir):

    if not is_valid_url(url):
        print(f"Invalid URL: {url}")
        return None
    
    file_name = url.split("/")[-1]
    file_path =  os.path.join(downloads_dir, file_name)

    if os.path.exists(file_path):
        print(f"File is already exists: {file_name}")
        return file_path
    
    try:
        # Send a GET requests to the URL with timeout
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()

        # Save the file
        with open(file_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

        print(f"Successfully downloaded: {file_name}")
        return file_path
    
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {str(e)}")
        return None
    

def extract_zip_file(zip_path, extract_path):
    if zip_path and zip_path.endswith('.zip'):
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                file_list = zip_ref.namelist()
                zip_ref.extractall(extract_path)
                print(f"Successfully extracted files from {os.path.basename(zip_path)}")
                extracted_files = [os.path.join(extract_path, f) for f in file_list]
                
                if extracted_files:
                    print(f"Extracted files: {extracted_files}")
                else:
                    print("No files were extracted")
            
            # Move os.remove outside the with block
            try:
                os.remove(zip_path)
                print(f"Successfully removed ZIP file: {os.path.basename(zip_path)}")
            except Exception as e:
                print(f"Error removing ZIP file {zip_path}: {str(e)}")
        
        except zipfile.BadZipFile:
            print(f"Error: {zip_path} is not valid ZIP file")
            return None
        except Exception as e:
            print(f"Error extracting {zip_path}: {str(e)}")
            return None

def main():
    downloads_dir = os.path.join(os.path.dirname(__file__), 'downloads')
    if not os.path.exists(downloads_dir):
        os.makedirs(downloads_dir)

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_url = {
            executor.submit(download_file, url, downloads_dir): url 
            for url in download_uris
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            downloaded_path = future.result()
            if downloaded_path:
                print(f"Processing download from {url}")
                extract_zip_file(downloaded_path, downloads_dir)

if __name__ == "__main__":
    main()
