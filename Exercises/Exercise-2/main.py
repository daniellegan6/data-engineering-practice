import requests
import pandas as pd
from bs4 import BeautifulSoup 
import os
from urllib.parse import urljoin
import sys
import importlib.util

exercise1_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'Exercise-1', 'main.py')
spec = importlib.util.spec_from_file_location("exercise1_main", exercise1_path)
exercise1_main = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exercise1_main)

download_file = exercise1_main.download_file
is_valid_url = exercise1_main.is_valid_url

def main():

    url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    download_dir = os.path.dirname(__file__)
    
    for row in soup.find_all("tr"):
        columns = row.find_all("td")
        if len(columns) >= 2 and columns[1].string:
            if "2024-01-19 10:27" in columns[1].string:
                URL_file_path = urljoin(url, columns[0].a["href"])
                break
    
    if URL_file_path:
        downloaded_path = download_file(URL_file_path, download_dir)
        df = pd.read_csv(downloaded_path)
        max_temp = df["HourlyDryBulbTemperature"].max()
        highest_temp_record = df[df['HourlyDryBulbTemperature'] == max_temp]
        print("Records with highest temperature:\n")
        print(highest_temp_record)


if __name__ == "__main__":
    main()
