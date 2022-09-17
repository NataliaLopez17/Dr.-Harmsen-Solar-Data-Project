"""
    [Created on October 8, 2021]

Authors:
    [Elias]: [Chevere]
    [Natalia]: [López]
"""
import time
import io
import sys
import os
import zipfile
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter, Retry

CONFIG = load_dotenv(".env")
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

download_url_list = []


"""
    timeout_http_adapter '[These are the headers to be used for the requests done 
    to each one of the nsrd API endpoints.]'
"""
headers = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache"
}


def timeout_http_adapter():
    """
    timeout_http_adapter [Takes the download link from the response in the request builder 
    function and then uses the link to download the file
    and retry if it does not find it.]

    Returns:
        [response]: [The download link that contains the file.]
    """
    retry_strategy = Retry(total=3, status_forcelist=[404, 400, 405, 403, 429, 500, 502, 503, 504],
                           method_whitelist=["HEAD", "GET", "OPTIONS", "POST"], backoff_factor=1)
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    print("fetching responses...")
    for url in download_url_list:
        responses = http.get(url, timeout=10)
        print(f"timeout_http_adapter() responses = {responses}")
    return responses


def request_builder(start=2018, end=2018, interval=15, email={default_email}):
    """
    request_builder [This function will generate the post request to the API endpoint 
    for a given set of years, and intervals.]

    Args:
        start (int, optional): [This is the desired starting year for the data acquisition. 
    The default for this parameter is 2018 as it is the minimum year available for data download.]. 
    Defaults to 2018.
        end ([type], optional): [The default value for this is the current year as that is 
    the limit for the API endpoint utilized. This parameter will determine until which 
    year you wish to gather data from.]. Defaults to datetime.now().year.
        interval (int, optional): [This will determine what interval of time the 
    data will be gathered in. The available intervals are: 30 and 60. 
    There is a possibility of 5 but the current request of data is too high for the 
    use of 5 minute interval.]. Defaults to 15.
        email (dict, optional): [This will be the email that the generated file will be 
    sent to after it has been created and is ready for download. ]. 
    Defaults to {default_email}.

    Returns:
        [response]: [This parameter will be the response of the request that will 
    serve to call the following function to download the file automatically.]
    """
    wk_list = [wktc, wktne, wktnw, wktse, wktsw, wktv]
    for i in wk_list:
        payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
            start, interval, attributes, email, i)
        response = requests.request(
            "POST", url_after_2018, data=payload, headers=headers)
        jsonResponse = response.json()
        time.sleep(60)
        download_url_list.append(
            jsonResponse['outputs']['downloadUrl'])
    return download_url_list, response


def download_zip(dest={destination}):
    """
    download_zip [This function will generate the post request to the API endpoint 
    for a given set of years, and intervals.]

    Args:
        dest (str, optional): [file destination to download the zip files]. 
        Defaults to {destination}.
    """
    for i in download_url_list:
        solar_res = requests.get(i)
        print(solar_res)
        with zipfile.ZipFile(io.BytesIO(solar_res.content)) as solar_zip_file:
            print("downloading zip file to destination....")
            solar_zip_file.extractall(dest)


if __name__ == "__main__":
    # Identify start_year and end_year as the years desired.
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    interval = sys.argv[3]
    email = sys.argv[4]
    items = range(10)

    if start_year.isdigit() and end_year.isdigit() and interval.isdigit():
        if int(start_year) >= 2018:
            response = request_builder(
                int(start_year), int(end_year), int(interval), email)
            print(response)
            time.sleep(60)
            if response[1].status_code == 200:
                time.sleep(60)
                print("waiting 10 minutes....")
                time.sleep(600)
                timeout_http_adapter()
                time.sleep(60)
                download_zip(destination)
        else:
            print("Make sure that the run command is in the form of \
                python <file_name> <start_year> <end_year> <time_interval> <email> \
                    where <start_year> >= 2018 and <end_year> is the same as <start_year>.")
