"""
Created on October 8, 2021

@author Elias Chevere
@author Natalia López
"""
import json
from datetime import datetime
import requests
import zipfile
import io
import sys
from dotenv import load_dotenv
import os
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

config = load_dotenv(".env")
url_after_2018 = os.getenv("URL_AFTER_2018")
attributes = os.getenv("ATTRIBUTES")
wkt = os.getenv("WKT1")
wkt2 = os.getenv("WKT2")
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
        method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
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


def initial_request_builder(start=2018, end=datetime.now().year, interval=30, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")

    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wkt)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def initial_request_builder2(start=2018, end=datetime.now().year, interval=30, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload2 = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                    attributes, email,
                                                                                                    wkt2)
    response2 = requests.request("POST", url_after_2018, data=payload2, headers=headers)
    return response2


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
        if int(start_year) >= 2018:
            response = initial_request_builder(int(start_year), int(end_year), int(interval), email)
            #response2 = initial_request_builder2(int(start_year), int(end_year), int(interval), email)
            print(response.json())
            #print(response2.json())
            if response.status_code == 200:
                print("passed third if")
                jsonResponse = response.json()
                #jsonResponse2 = response2.json()
                download_url = jsonResponse["outputs"]["downloadUrl"]
                print(download_url)
                #download_url2 = jsonResponse2["downloadUrl"]
                TimeoutHTTPAdapter(download_url)
                #TimeoutHTTPAdapter(download_url2)
                download_zip(download_url, destination)
        else:
            print("Enter a valid year starting at 2018 after the script name.")
