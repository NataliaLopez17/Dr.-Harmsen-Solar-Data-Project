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
wktnw = os.getenv("WKTNW")
wktne = os.getenv("WKTNE")
wktsw = os.getenv("WKTSW")
wktse = os.getenv("WKTSE")
wktc = os.getenv("WKTC")
wktv = os.getenv("WKTV")
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

#url_nw, url_ne, url_sw, url_se, url_c, url_v
def TimeoutHTTPAdapter(url_ne):
    retry_strategy = Retry(
        total=3,
        status_forcelist=[404, 400, 405, 403, 429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=10
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    print("passed http.mount", "\n")
    #response_nw = http.get(url_nw, timeout=10)
    response_ne = http.get(url_ne, timeout=10)
    print("response passed", "\n")
    # response_sw = http.get(url_sw, timeout=10)
    # print("response sw passed")
    # response_se = http.get(url_se, timeout=10)
    # response_c = http.get(url_c, timeout=10)
    # response_v = http.get(url_v, timeout=10)
    # return response_nw  response_ne, response_sw, response_se, response_c, response_v
    return response_ne


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


def nw_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")

    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktnw)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def ne_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktne)
    print("payload= ", payload, "\n")                                                                                        
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def sw_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktsw)
    print("payload=",payload)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def se_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktse)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def c_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktc)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
    return response


def v_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(years, interval,
                                                                                                   attributes, email,
                                                                                                   wktv)
    response = requests.request("POST", url_after_2018, data=payload, headers=headers)
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

#download_url_ne, download_url_sw, download_url_se,download_url_c, download_url_v,
def download_zip(download_url_ne, dest={destination}):
    # solar_res_nw = requests.get(download_url_nw)
    solar_res_ne = requests.get(download_url_ne)
    #solar_res_sw = requests.get(download_url_sw)
    # solar_res_se = requests.get(download_url_se)
    # solar_res_c = requests.get(download_url_c)
    # solar_res_v = requests.get(download_url_v)


    # zip_file_nw = zipfile.ZipFile(io.BytesIO(solar_res_nw.content))
    zip_file_ne = zipfile.ZipFile(io.BytesIO(solar_res_ne.content))
    #zip_file_sw = zipfile.ZipFile(io.BytesIO(solar_res_sw.content))
    # zip_file_se = zipfile.ZipFile(io.BytesIO(solar_res_se.content))
    # zip_file_c = zipfile.ZipFile(io.BytesIO(solar_res_c.content))
    # zip_file_v = zipfile.ZipFile(io.BytesIO(solar_res_v.content))


    # zip_file_nw.extractall(dest)
    zip_file_ne.extractall(dest)
    #zip_file_sw.extractall(dest)
    # zip_file_se.extractall(dest)
    # zip_file_c.extractall(dest)
    # zip_file_v.extractall(dest)


if __name__ == "__main__":
    # Identify the current year desired.
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    interval = sys.argv[3]
    email = sys.argv[4]
    items = range(10)
    if start_year.isdigit() and end_year.isdigit() and interval.isdigit():
        if int(start_year) >= 2018:
            # response_nw = nw_request_builder(int(start_year), int(end_year), int(interval), email)
            response_ne = ne_request_builder(int(start_year), int(end_year), int(interval), email)
            print("response_ne", response_ne, "\n")
            # response_sw = sw_request_builder(int(start_year), int(end_year), int(interval), email)
            # print("response sw", response_sw, "\n")
            # response_se = se_request_builder(int(start_year), int(end_year), int(interval), email)
            # response_c = c_request_builder(int(start_year), int(end_year), int(interval), email)
            # response_v = v_request_builder(int(start_year), int(end_year), int(interval), email)

            if response_ne.status_code == 200: #and response_ne.status_code == 200 \
                    #and response_sw.status_code == 200 and response_se.status_code == 200 \
                    #and response_c.status_code == 200 and response_v.status_code == 200:
                # jsonResponse_nw = response_nw.json()
                jsonResponse_ne = response_ne.json()
                # jsonResponse_sw = response_sw.json()
                # jsonResponse_se = response_se.json()
                # jsonResponse_c = response_c.json()
                # jsonResponse_v = response_v.json()
                # print("json response nw", jsonResponse_nw, "\n")
                print("json response ne", jsonResponse_ne, "\n")
                # print("json response sw", jsonResponse_sw, "\n")
                # print("json response se", jsonResponse_se, "\n")
                # print("json response c", jsonResponse_c, "\n")
                # print("json response v", jsonResponse_v, "\n")

                # download_url_nw = jsonResponse_nw['outputs']['downloadUrl']
                download_url_ne = jsonResponse_ne['outputs']['downloadUrl']
                # download_url_sw = jsonResponse_sw['outputs']['downloadUrl']
                # download_url_se = jsonResponse_se['outputs']['downloadUrl']
                # download_url_c = jsonResponse_c['outputs']['downloadUrl']
                # download_url_v = jsonResponse_v['outputs']['downloadUrl']
                # print("download_url nw", download_url_nw, "\n")
                print("download_url ne", download_url_ne, "\n")
                # print("download_url sw", download_url_sw, "\n")
                # print("download_url se", download_url_se, "\n")
                # print("download_url c", download_url_c, "\n")
                # print("download_url v", download_url_v, "\n")

                TimeoutHTTPAdapter(download_url_ne)
                download_zip(download_url_ne,destination)
        else:
            print("Enter a valid year starting at 2018 after the script name.")
