"""
    [Created on October 8, 2021]

Authors:
    [Elias]: [Chevere]
    [Natalia]: [López]
"""
import time
from datetime import datetime
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


def timeout_http_adapter(url_nw, url_ne, url_sw):
    """
    timeout_http_adapter [Takes the download link from the response in the request builder 
    function and then uses the link to download the file
    and retry if it does not find it.]

    Parameters:
        [url]: [The download link returned from the request builder functions.]

    Returns:
        [response]: [The download link that contains the file.]
    """

    retry_strategy = Retry(
        total=3,
        status_forcelist=[404, 400, 405, 403, 429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    print("fetching responses for nw, ne and sw...")
    response_nw = http.get(url_nw, timeout=10)
    response_ne = http.get(url_ne, timeout=10)
    response_sw = http.get(url_sw, timeout=10)
    return response_nw, response_ne, response_sw


def timeout_http_adapter2(url_se, url_c, url_v):
    """
    timeout_http_adapter2 [Takes the download link from the response in the request builder 
    function and then uses the link to download the file
    and retry if it does not find it.]

    Parameters:
        [url]: [The download link returned from the request builder functions.]

    Returns:
        [response]: [The download link that contains the file.]
    """
    retry_strategy = Retry(
        total=3,
        status_forcelist=[404, 400, 405, 403, 429, 500, 502, 503, 504],
        method_whitelist=["HEAD", "GET", "OPTIONS", "POST"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    print("fetching responses for se, c and v...")
    response_se = http.get(url_se, timeout=10)
    response_c = http.get(url_c, timeout=10)
    response_v = http.get(url_v, timeout=10)
    return response_se, response_c, response_v


"""
    timeout_http_adapter2 '[These are the headers to be used for the requests done 
    to each one of the nsrd API endpoints.]'
"""
headers = {
    'content-type': "application/x-www-form-urlencoded",
    'cache-control': "no-cache"
}


def nw_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    nw_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")

    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktnw)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def ne_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    ne_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktne)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def sw_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    sw_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktsw)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def se_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    se_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktse)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def c_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    c_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktc)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def v_request_builder(start=2018, end=datetime.now().year, interval=15, email={default_email}):
    """
    v_request_builder [This function will generate the post request to the API endpoint 
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
    years = ""
    for year in range(int(start), int(end + 1)):
        years = years + str(year) + ","
    years = years.rstrip(",")
    payload = "names={}&leap_day=false&interval={}&utc=false&attributes={}&email={}&wkt={}".format(
        years, interval, attributes, email, wktv)
    response = requests.request(
        "POST", url_after_2018, data=payload, headers=headers)
    return response


def download_zip(download_url_nw, download_url_ne, download_url_sw, dest={destination}):
    """
    download_zip [This function will generate the post request to the API endpoint 
    for a given set of years, and intervals.]

    Args:
        download_url_nw ([string]): [This will be the url utilized to download the 
    file generated with the data of the northwestern corner of PR.]
        download_url_ne ([string]): [This will be the url utilized to download the 
    file generated with the data of the northeastern corner of PR.]
        download_url_sw ([string]): [This will be the url utilized to download the 
    file generated with the data of the southwestern corner of PR.]
        dest (str, optional): [file destination to download the zip files]. 
    Defaults to {destination}.
    """
    solar_res_nw = requests.get(download_url_nw)
    solar_res_ne = requests.get(download_url_ne)
    solar_res_sw = requests.get(download_url_sw)

    with zipfile.ZipFile(io.BytesIO(solar_res_nw.content)) as zip_file_nw:
        print("downloading nw zip file to destination....")
        zip_file_nw.extractall(dest)
    with zipfile.ZipFile(io.BytesIO(solar_res_ne.content)) as zip_file_ne:
        print("downloading ne zip file to destination....")
        zip_file_ne.extractall(dest)
    with zipfile.ZipFile(io.BytesIO(solar_res_sw.content)) as zip_file_sw:
        print("downloading sw zip file to destination....")
        zip_file_sw.extractall(dest)


def download_zip2(download_url_se, download_url_c, download_url_v, dest={destination}):
    """
    download_zip2 [This function will generate the post request to the API endpoint 
    for a given set of years, and intervals.]

    Args:
        download_url_se ([string]): [This will be the url utilized to download the 
    file generated with the data of the southeastern corner of PR.]
        download_url_c ([string]): [This will be the url utilized to download the 
    file generated with the data of the island of Culebra.]
        download_url_v ([string]): [This will be the url utilized to download the 
    file generated with the data of the island of Vieques.]
        dest (str, optional): [file destination to download the zip files]. 
    Defaults to {destination}.
    """
    solar_res_se = requests.get(download_url_se)
    solar_res_c = requests.get(download_url_c)
    solar_res_v = requests.get(download_url_v)

    with zipfile.ZipFile(io.BytesIO(solar_res_se.content)) as zip_file_se:
        print("downloading se zip file to destination....")
        zip_file_se.extractall(dest)

    with zipfile.ZipFile(io.BytesIO(solar_res_c.content)) as zip_file_c:
        print("downloading c zip file to destination....")
        zip_file_c.extractall(dest)

    with zipfile.ZipFile(io.BytesIO(solar_res_v.content)) as zip_file_v:
        print("downloading v zip file to destination....")
        zip_file_v.extractall(dest)


if __name__ == "__main__":
    # Identify the current year desired.
    start_year = sys.argv[1]
    end_year = sys.argv[2]
    interval = sys.argv[3]
    email = sys.argv[4]
    items = range(10)
    print("In the command prompt please type \
          python <file_name> <start_year> <end_year> <time_interval> <email>")

    if start_year.isdigit() and end_year.isdigit() and interval.isdigit():
        if int(start_year) >= 2018:

            response_nw = nw_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_nw", response_nw.content, "\n")

            time.sleep(60)

            response_ne = ne_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_ne", response_ne.content, "\n")

            time.sleep(60)

            response_sw = sw_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_sw", response_sw.content, "\n")

            time.sleep(60)

            response_se = se_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_se", response_se.content, "\n")

            time.sleep(60)

            response_c = c_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_c", response_c.content, "\n")

            time.sleep(60)

            response_v = v_request_builder(
                int(start_year), int(end_year), int(interval), email)
            print("response_v", response_v.content, "\n")

            if response_sw.status_code == 200 and response_ne.status_code == 200:
                if response_sw.status_code == 200 and response_se.status_code == 200:
                    if response_c.status_code == 200 and response_v.status_code == 200:
                        jsonResponse_nw = response_nw.json()
                        jsonResponse_ne = response_ne.json()
                        jsonResponse_sw = response_sw.json()
                        jsonResponse_se = response_se.json()
                        jsonResponse_c = response_c.json()
                        jsonResponse_v = response_v.json()

                        download_url_nw = jsonResponse_nw['outputs']['downloadUrl']
                        download_url_ne = jsonResponse_ne['outputs']['downloadUrl']
                        download_url_sw = jsonResponse_sw['outputs']['downloadUrl']
                        download_url_se = jsonResponse_se['outputs']['downloadUrl']
                        download_url_c = jsonResponse_c['outputs']['downloadUrl']
                        download_url_v = jsonResponse_v['outputs']['downloadUrl']

                        print("waiting 40 minutes....zzz....")

                        time.sleep(2400)

                        timeout_http_adapter(
                            download_url_nw, download_url_ne, download_url_sw)
                        print("waiting 40 minutes....zzz....")

                        time.sleep(2400)

                        timeout_http_adapter2(
                            download_url_se, download_url_c, download_url_v)
                        print("waiting 1 minute....zzz....")

                        time.sleep(60)

                        download_zip(download_url_nw, download_url_ne,
                                     download_url_sw, destination)
                        print("waiting 1 minute....zzz....")

                        time.sleep(60)

                        download_zip2(download_url_se, download_url_c,
                                      download_url_v, destination)
        else:
            print("Make sure that the run command is in the form of \
                python <file_name> <start_year> <end_year> <time_interval> <email> \
                    where <start_year> >= 2018.")
