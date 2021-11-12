"""
Created on October 8, 2021

@author Elias Chevere
@author Natalia López
"""

from datetime import datetime
import requests
import zipfile
import io
import sys
from requests.exceptions import Timeout
from dotenv import load_dotenv
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import time

config = load_dotenv(".env")

url_after_2018 = os.getenv("URL_AFTER_2018")
url_until_2017 = os.getenv("URL_UNTIL_2017")

attributes = os.getenv("ATTRIBUTES")

wkt = os.getenv("WKT")
default_email = os.getenv("EMAIL")
api_key = os.getenv("API_KEY")
destination = os.getenv('DESTINATION')

'''
Takes the download link from the response in the request builder function and then uses the link to download the file
and retry if it does not find it.
---------------------------------------------------------------------------------------------------------------------
Parameters:
url: The download link returned from the request builder functions.
---------------------------------------------------------------------------------------------------------------------
Returns:
response: The download link that contains the file.
'''


def TimeoutHTTPAdapter(url):
    retry_strategy = Retry(
        total=3,
        status_forcelist=[404, 400, 405, 403, 429, 500, 502, 503, 504],
        #method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("http://", adapter)
    response = http.get(url, timeout=18000)
    return response


'''
These are the headers to be used for the requests done to each one of the nsrd API endpoints.
'''
headers = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache"
}

'''
This function will generate the post request to the API endpoint for a given set of years, and intervals. 
___________________________________________________________________
Parameters:
start: This is the desired starting year for the data acquisition.
end: The default value for this is 2017 as that is the limit for the API endpoint utilized. 
This parameter will determine until which year you wish to gather data from.
interval: This will determine what interval of time the data will be gathered in. 
The available intervals are: 30 and 60. 
There is a possibility of 5 but the current request of data is too high for the use of 5 minute interval.
email: This will be the email that the generated file will be sent to after it has been created 
and is ready for download. 
___________________________________________________________________
Return:
response: This parameter will be the response of the request that will serve to call the following 
function to download the file automatically.
'''


def initial_request_builder(start, end=2017, interval=30, email={default_email}):
    attributes_list = attributes.split(",")
    payload = []
    years = []
    if start.isdigit():
        if int(end) == int(start):
            for i in range(1):
                years.append(str(start))
                years.append(str(end))
                payload.append(f"names={years}&leap_day=false&interval={interval}"
                               f"&utc=false&attributes={attributes_list}&email={email}&wkt={wkt}")
        elif int(end) - int(start) == 1:
            for year in range(2):
                print("reeee3")
                payload.append(f"email={email}&wkt={wkt}&names={start,end}&attributes={attributes}&leap_day=false&utc=false&interval={interval}")
        elif int(end) - int(start) == 2:
            for year in range(3):
                print("reeee4")
                payload.append(f"email={email}&wkt={wkt}&names={int(start),2016,end}&attributes={attributes}&leap_day=false&utc=false&interval={interval}")

    print("payload = ", payload[0])
    response = requests.request("POST", url_until_2017, json=payload[0], headers=headers)
    #print(url_until_2017 + payload[0])
    return response


'''
This function will generate the post request to the API endpoint for a given set of years, and intervals. 
___________________________________________________________________
Parameters:
start: This is the desired starting year for the data acquisition. The default for this parameter 
is 2018 as it is the minimum year available for data download. 
end: The default value for this is the current year as that is the limit for the API endpoint utilized. 
This parameter will determine until which year you wish to gather data from.
interval: This will determine what interval of time the data will be gathered in. 
The available intervals are: 30 and 60. There is a possibility of 5 but the current request of data is 
too high for the use of 5 minute interval.
email: This will be the email that the generated file will be sent to after it has been 
created and is ready for download. 
___________________________________________________________________
Return:
response: This parameter will be the response of the request that will serve to call the following function to download the file automatically.
'''


def initial_request_builder_2018(start=2018, end=datetime.now().year, interval=30, email={default_email}):
    payload = [
        f"names={year}&leap_day=false&interval={interval}&utc=false&email={email}\\&attributes={attributes}&wkt={wkt}"
        for year in range(start, end)]
    response = requests.request("GET", url_after_2018, data=payload, headers=headers)
    print("2018 response\n", response.content)
    return response


def wait():
    wait()


'''
This function will generate the post request to the API endpoint for a given set of years, and intervals. 
___________________________________________________________________
Parameters:
download_url: This will be the url utilized to download the file generated with the desired data.
___________________________________________________________________
'''


def download_zip(download_url, dest={destination}):
    solar_res = requests.get(download_url)
    zip_file = zipfile.ZipFile(io.BytesIO(solar_res.content))
    zip_file.extractall(dest)


if __name__ == "__main__":
    # Identify the current year desired.
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    interval = sys.argv[3]
    email = sys.argv[4]
    if start_year.isdigit() and end_year.isdigit() and interval.isdigit():
        if 2017 >= int(start_year) >= 1998:
            response = initial_request_builder(start_year, int(end_year), int(interval), email)
            print(response.json())
            if response.status_code == 200:
                print("passed third if")
                jsonResponse = response.json()
                download_url = jsonResponse["download_url"]
                TimeoutHTTPAdapter(download_url)
        elif int(start_year) >= 2018:
            if len(sys.argv) == 4:
                initial_request_builder_2018(int(sys.argv[1]), int(sys.argv[2]), int(sys.argv[3]), sys.argv[4])
            elif len(sys.argv) == 3:
                initial_request_builder_2018(int(sys.argv[1]), int(sys.argv[3]), sys.argv[4])
            elif len(sys.argv) == 2:
                initial_request_builder_2018(int(sys.argv[1]), sys.argv[4])
            elif len(sys.argv) == 1:
                initial_request_builder_2018(int(sys.argv[1]))

        else:
            print("Enter a valid year after the script name.")
