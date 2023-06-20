import requests
import json
from xml.dom import minidom
import urllib.request as ur
from datetime import datetime
import pandas as pd
import os
import json
# from dags.scripts.utils.logs import logger

# Libraries for web scraping

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

# For gpt data
import re
import calendar
import ssl
import shutil
import asyncio
import aiohttp
import aiofiles


#######################
# PARAMS FOR GPT DATA #
# TODO: Pasar parametros.
# Interval for regular Data Collection
city = "Transit_all"
# input_file = "/dags/data/gpt_data/Transit_ID.csv"
# raw_data_path = "/dags/data/cache/"

# Change the header to your System/ PC
USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15"}


# noinspection PyUnresolvedReferences
cookies = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}

gcontext = ssl.SSLContext(ssl.PROTOCOL_TLS)

#######################


def _now_times() -> tuple:
    """
    This function returns the date and hour of the data.
    Args:
        None
    Returns:
        date (str): date of the data
        hour (str): hour of the data
    """
    now = datetime.now()
    date = now.strftime("%Y%m%d")
    hour = now.strftime("%H")
    return date, hour


def _converting_and_saving_data(
    data: pd.DataFrame,
    columns: list,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    date: str,
    hour: str,
):
    """
    This function saves the data in a csv file.
    Args:
        df (pandas dataframe): dataframe with the data
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        date (str): date of the data
        hour (str): hour of the data
    Returns:
        None
        """
    df = pd.DataFrame(data, columns=columns)

    dir_path = airflow_home + location_data + sublocation_data
    file_path = dir_path + date + "_" + hour + "_" + file_name + ".csv"

    df.to_csv(file_path, index=False)

#TODO: Explicar subfunciones.
async def _download_site(session, url, place, timestr, count, airflow_home, location_data, sublocation_cache):
    try:
        response = await session.request(method="GET", url=url, ssl=gcontext)
        if response.status != 200:
            print(f"Failed to download {url}, status code: {response.status}")
            return
        filename = (airflow_home + location_data + sublocation_cache + city + "/" + timestr + "/" + str(place) + ".txt"
        )
        print(count, place)

        # Ensure directory exists before writing to it
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        async with aiofiles.open(filename, "ba") as f:
            async for content in response.content.iter_chunked(1024):
                await f.write(content)
        return filename
    except Exception as e:
        print(f"An error occurred with {url}: {str(e)}")


async def _download_all_sites(timestr, places, airflow_home, location_data, sublocation_cache):
    timeout = aiohttp.ClientTimeout(total=60 * 60)
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(
        connector=connector, headers=USER_AGENT, cookies=cookies, timeout=timeout
    ) as session:
        tasks = []
        count = 0
        for place_identifier in places:
            count += 1
            if place_identifier[1]:
                search_url = (
                    "https://www.google.com/maps/place/?q=place_id:"
                    + place_identifier[0]
                    + "&hl=en"
                )
            else:
                search_url = place_identifier[0]
            task_asyn = asyncio.ensure_future(
                _download_site(session, search_url, place_identifier[2], timestr, count, airflow_home, location_data, sublocation_cache)
            )
            tasks.append(task_asyn)
        await asyncio.gather(*tasks, return_exceptions=True)


def _get_populartimes_from_search(place_identifier, path):
    """
    based on https://github.com/m-wrzr/populartimes
    request information for a place and parse current popularity
    :param place_identifier: name and address string
    :return:
    """

    with open(path + "/" + str(place_identifier) + ".txt", mode="rb") as d:
        content = d.read()

    data = content.decode('utf-8').split('/*""*/')[0]
    try:

        idx1 = data.index('APP_INITIALIZATION_STATE=')
        data_new = data[idx1 + 25:]
        idx2 = data_new.index(";window.APP_FLAGS=")

        final_string = data_new[:idx2]

        d = final_string.split("""'\\n""")
        final_string = d[2].replace('\\\\\\', '#?replace?#')
        final_string = final_string.replace('\\', '')
        final_string = final_string.replace('#?replace?#', '\\')
        final_string = '[[' + final_string.replace(']"]', ']]')
        flag_error = True
        count_error = 0
        while flag_error and count_error < 250:
            count_error += 1
            try:
                jdata = json.loads(r'' + final_string)
                flag_error = False
            except Exception as e:
                msg = e.msg
                if ("Expecting ',' delimiter" in e.msg):
                    final_string = final_string[:e.pos - 1] + '' + final_string[e.pos:]
                else:
                    final_string = final_string[:e.pos] + '' + final_string[e.pos + 1:]

        if (count_error == 250):
            raise Exception(
                'Error from load Json ' + msg + '  https://www.google.com/maps/place/?q=place_id:' + place_identifier)
        info = _index_get(jdata, 0, 0, 6)

        rating = _index_get(info, 4, 7)
        rating_n = _index_get(info, 4, 8)

        popular_times = _index_get(info, 84, 0)

        # current_popularity is also not available if popular_times isn't
        current_popularity = _index_get(info, 84, 7, 1)

        time_spent = _index_get(info, 117, 0)

        # extract wait times and convert to minutes
        if time_spent:

            nums = [float(f) for f in re.findall(r'\d*\.\d+|\d+', time_spent.replace(",", "."))]
            contains_min, contains_hour = "min" in time_spent, "hour" in time_spent or "hr" in time_spent

            time_spent = None

            if contains_min and contains_hour:
                time_spent = [nums[0], nums[1] * 60]
            elif contains_hour:
                time_spent = [nums[0] * 60, (nums[0] if len(nums) == 1 else nums[1]) * 60]
            elif contains_min:
                time_spent = [nums[0], nums[0] if len(nums) == 1 else nums[1]]

            time_spent = [int(t) for t in time_spent]

        return rating, rating_n, popular_times, current_popularity, time_spent
    except Exception as e:
        raise Exception(repr(e))
    # return  None,None,None,None,None


def _get_popularity_for_day(popularity):
    """
    based on https://github.com/m-wrzr/populartimes
    Returns popularity for day
    :param popularity:
    :return:
    """

    # Initialize empty matrix with 0s
    pop_json = [[0 for _ in range(24)] for _ in range(7)]
    wait_json = [[0 for _ in range(24)] for _ in range(7)]

    for day in popularity:

        day_no, pop_times = day[:2]

        if pop_times:
            for hour_info in pop_times:

                hour = hour_info[0]
                pop_json[day_no - 1][hour] = hour_info[1]

                # check if the waiting string is available and convert no minutes
                if len(hour_info) > 5:
                    wait_digits = re.findall(r'\d+', hour_info[3])

                    if len(wait_digits) == 0:
                        wait_json[day_no - 1][hour] = 0
                    elif "min" in hour_info[3]:
                        wait_json[day_no - 1][hour] = int(wait_digits[0])
                    elif "hour" in hour_info[3]:
                        wait_json[day_no - 1][hour] = int(wait_digits[0]) * 60
                    else:
                        wait_json[day_no - 1][hour] = int(wait_digits[0]) * 60 + int(wait_digits[1])

                # day wrap
                if hour_info[0] == 23:
                    day_no = day_no % 7 + 1

    ret_popularity = [
        {
            "name": list(calendar.day_name)[d],
            "data": pop_json[d]
        } for d in range(7)
    ]

    # waiting time only if applicable
    ret_wait = [
        {
            "name": list(calendar.day_name)[d],
            "data": wait_json[d]
        } for d in range(7)
    ] if any(any(day) for day in wait_json) else []

    # {"name" : "monday", "data": [...]} for each weekday as list
    return ret_popularity, ret_wait



def _index_get(array, *argv):
    """
    based on https://github.com/m-wrzr/populartimes
    checks if a index is available in the array and returns it
    :param array: the data array
    :param argv: index integers
    :return: None if not available or the return value
    """
    try:
        for index in argv:
            array = array[index]

        return array

    # there is either no info available or no popular times
    # TypeError: rating/rating_n/populartimes wrong of not available
    except (IndexError, TypeError):
        return None



#######################

def test_webdriver(url: str, chromedriver_path: str):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=DEBUG")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)
    try:
        driver.get(url)
        time.sleep(2)
        print(driver.title)
    finally:
        driver.quit()


#######################


def extraction_bike_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
    """
    This function extracts data from the JCDecaux API and saves it in a csv file.
    Args:
        url (str): url of the JCDecaux API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    date, hour = _now_times()
    response = requests.get(url)
    data = json.loads(response.text)

    rows = []
    for el in data:

        row = []
        name = el["name"].replace("'", "")
        lat = el["position"]["lat"]
        long = el["position"]["lng"]
        bike_stands = el["bike_stands"]
        available_bikes = el["available_bikes"]
        available_bike_stands = el["available_bike_stands"]

        row.append(name)
        row.append(date)
        row.append(hour)
        row.append(lat)
        row.append(long)
        row.append(bike_stands)
        row.append(available_bikes)
        row.append(available_bike_stands)
        rows.append(row)

    _converting_and_saving_data(rows, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)


def extraction_charging_station_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
    """
    This function extracts data from the Public.lu API and saves it in a csv file.
    Args:
        url (str): url of the Public.lu API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    date, hour = _now_times()

    dom = minidom.parse(ur.urlopen(url))
    owned = dom.getElementsByTagName('Placemark')

    rows = []
    for el in owned:
        row = []

        lat = el.getElementsByTagName('coordinates')[0].firstChild.data.split(
            ',')[1]
        long = el.getElementsByTagName('coordinates')[0].firstChild.data.split(
            ',')[0]
        address = el.getElementsByTagName(
            'address')[0].firstChild.nodeValue.replace(",",
                                                       "").replace("'", "")
        occupied = el.getElementsByTagName(
            'description')[0].firstChild.nodeValue.split(
                'occupied connectors')[0].split('</b>')[-2].split('<b>')[-1]
        available = el.getElementsByTagName(
            'description')[0].firstChild.nodeValue.split(
                'available connectors')[0].split('</b>')[-2].split('<b>')[-1]

        row.append(date)
        row.append(hour)
        row.append(lat)
        row.append(long)
        row.append(address)
        row.append(occupied)
        row.append(available)
        rows.append(row)

    _converting_and_saving_data(rows, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)


def extraction_traffic_counter_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
    """
    This function extracts data from the Cita.lu API and saves it in a csv file.
    Args:
        url (str): url of the Public.lu API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    date, hour = _now_times()

    dom = minidom.parse(ur.urlopen(url))
    owned = dom.getElementsByTagName('siteMeasurements')

    rows = []
    for el in owned:
        row = []

        id = el.getElementsByTagName(
            'measurementSiteReference')[0].getAttribute("id")
        latitude = el.getElementsByTagName('latitude')[0].firstChild.nodeValue
        longitude = el.getElementsByTagName(
            'longitude')[0].firstChild.nodeValue
        road = el.getElementsByTagName('roadNumber')[0].firstChild.nodeValue
        direction_tags = dom.getElementsByTagName(
            'directionBoundOnLinearSection')
        if direction_tags:
            direction = direction_tags[0].firstChild.nodeValue
        else:
            direction = None

        percentage = el.getElementsByTagName(
            'percentage')[0].firstChild.nodeValue
        speed = el.getElementsByTagName('speed')[0].firstChild.nodeValue
        vehicle_flow_rate = el.getElementsByTagName(
            'vehicleFlowRate')[0].firstChild.nodeValue

        row.append(date)
        row.append(hour)
        row.append(id)
        row.append(latitude)
        row.append(longitude)
        row.append(road)
        row.append(direction)
        row.append(percentage)
        row.append(speed)
        row.append(vehicle_flow_rate)
        rows.append(row)

    _converting_and_saving_data(rows, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)


def extraction_parking_data(url: str, chromedriver_path: str, airflow_home: str, location_data: str, sublocation_data: str, file_name: str,
                          columns: list) -> None:
    """
    This function extracts data from the parking website of the Luxembourg City and saves it in a csv file.
    Args:
        url (str): url of the parking website
        chromedriver_path (str): path to chromedriver
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    date, hour = _now_times()

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=DEBUG")
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(executable_path=chromedriver_path, options=chrome_options)

    try:
        driver.get(url)
        time.sleep(10)
        url_elems = driver.find_elements("xpath", "//div[@class='panel-header']")

        rows = []
        for father in url_elems:
            row = []
            row.append(date)
            row.append(hour)
            name = father.find_element("xpath", ".//span[@class='h6 panel-main-title']")
            b = father.find_element("xpath", ".//div[@class='parking-data']")
            time.sleep(2)
            row.append(name.text.replace("â", "a").replace("é", "e").replace("'", ""))

            try:
                if b.text == 'No data available':
                    row.extend([-1] * 3)
                    row.extend(['No data available'])
                    rows.append(row)
                elif b.text == 'Closed':
                    row.extend([-1] * 3)
                    row.extend(['Closed'])
                    rows.append(row)
                else:
                    p = b.text.split()
                    available = p[0]
                    total = p[-1].replace('/', '')
                    occupancy = 100 - ((float(available) / float(total)) * 100)
                    occupancy = round(occupancy, 2)
                    ac = father.find_element("xpath", ".//*[local-name() = 'svg' and @width='12']")

                    row.append(available)
                    row.append(total)
                    row.append(occupancy)
                    row.append(ac.get_attribute('class'))

                    rows.append(row)

            except Exception as e:
                row.extend([-1] * 3)
                row.extend(['Error'])
                rows.append(row)

        
        _converting_and_saving_data(rows, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)

    finally:
        driver.quit()


def extraction_gpt_data(airflow_home: str, location_data: str, sublocation_cache: str, sublocation_data: str, file_name: str,
                          columns: list) -> None:
    """
    This function extracts data from the Google Popular Times website and saves it in a csv file.
    Args:
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    date, hour = _now_times()

    try:
        timestr = time.strftime("%Y%m%d-%H")

        # Directory for raw data
        try:
            os.mkdir(airflow_home + location_data + sublocation_cache)
            # os.mkdir(raw_data_path[:-1])
        except FileExistsError:
            pass

        # Place to save the data
        dir_path_gpt = airflow_home + location_data + sublocation_cache + city + "/"
        # dir_path_gpt = raw_data_path + city + "/"
        try:
            os.mkdir(dir_path_gpt[:-1])
        except FileExistsError:
            pass
        try:
            os.mkdir(dir_path_gpt + timestr)
        except FileExistsError:
            pass
        
        #TODO: Incluir nuevos lugares y todo para params.py
        # input file with list of POIs with names, house number, street, etc
        dict_df = {
                    'index': [1],
                    'type': ['Luxembourg_transit'],
                    'place_id': ['ChIJ32pqnLkVlUcRxbSuQU7UQcU'],
                    'name': ['Apach'],
                    'Link': [''],
                    'lat': [49.458944],
                    'lng': [6.3722829],
                    'Type.1': ['train_station'],
                    'Type.2': ['transit_station'],
                    'Type.3': ['point_of_interest'],
                    'Type.4': ['establishment'],
                    'Type.5': ['']
                }
        df = pd.DataFrame(dict_df)

        list_places = []
        place_ids_list = list(df["place_id"])
        link_list = list(df['Link'])
        ind_list = list(df["index"])

        for i in range(len(place_ids_list)):
            place = place_ids_list[i]
            if place == "nan":
                place = link_list[i]
                list_places.append([place, False, ind_list[i]])
            else:
                list_places.append([place, True, ind_list[i]])

        start_time = time.time()

        asyncio.run(_download_all_sites(timestr, list_places, airflow_home, location_data, sublocation_cache))


        list_rating = []
        list_rating_n = []
        list_current_popularity = []
        list_waittimes = []
        list_populartimes_monday = []
        list_populartimes_tuesday = []
        list_populartimes_wednesday = []
        list_populartimes_thursday = []
        list_populartimes_friday = []
        list_populartimes_saturday = []
        list_populartimes_sunday = []

        for i in range(len(list_places)):
            try:

                (
                    rating,
                    rating_n,
                    popular_times,
                    current_popularity,
                    time_spent,
                ) = _get_populartimes_from_search(list_places[i][2], dir_path_gpt + timestr)
                formatted_pp_times, formatted_wait_time = _get_popularity_for_day(
                    popular_times
                )

                # Create a dictionary to store the separate columns
                formatted_pp_times_columns = {}

                # Iterate through the list of dictionaries
                for day_data in formatted_pp_times:
                    day_name = day_data["name"]
                    day_values = day_data["data"]
                    
                    # Create a new column name based on the day name
                    column_name = f"formatted_pp_times_{day_name.lower()}"
                    
                    # Assign the day values to the respective column name
                    formatted_pp_times_columns[column_name] = day_values

                list_populartimes_monday.append(formatted_pp_times_columns["formatted_pp_times_monday"])
                list_populartimes_tuesday.append(formatted_pp_times_columns["formatted_pp_times_tuesday"])
                list_populartimes_wednesday.append(formatted_pp_times_columns["formatted_pp_times_wednesday"])
                list_populartimes_thursday.append(formatted_pp_times_columns["formatted_pp_times_thursday"])
                list_populartimes_friday.append(formatted_pp_times_columns["formatted_pp_times_friday"])
                list_populartimes_saturday.append(formatted_pp_times_columns["formatted_pp_times_saturday"])
                list_populartimes_sunday.append(formatted_pp_times_columns["formatted_pp_times_sunday"])
                list_rating.append(rating)
                list_rating_n.append(rating_n)
                list_current_popularity.append(current_popularity)
                list_waittimes.append(time_spent)

            except Exception as e:
                print(
                    timestr
                    + " "
                    + repr(e)
                    + " "
                    + "https://www.google.com/maps/place/?q=place_id:"
                    + list_places[i][0],
                )
                list_populartimes_monday.append("None")
                list_populartimes_tuesday.append("None")
                list_populartimes_wednesday.append("None")
                list_populartimes_thursday.append("None")
                list_populartimes_friday.append("None")
                list_populartimes_saturday.append("None")
                list_populartimes_sunday.append("None")
                list_rating.append("None")
                list_rating_n.append("None")
                list_current_popularity.append("None")
                list_waittimes.append("None")

        df['date'] = date
        df['hour'] = hour
        df['city'] = city
        df["rating"] = list_rating
        df["rating_n"] = list_rating_n
        df['popularity_monday'] = str(list_populartimes_monday).replace(',','')
        df['popularity_tuesday'] = str(list_populartimes_tuesday).replace(',','')
        df['popularity_wednesday'] = str(list_populartimes_wednesday).replace(',','')
        df['popularity_thursday'] = str(list_populartimes_thursday).replace(',','')
        df['popularity_friday'] = str(list_populartimes_friday).replace(',','')
        df['popularity_saturday'] = str(list_populartimes_saturday).replace(',','')
        df['popularity_sunday'] = str(list_populartimes_sunday).replace(',','')
        df['live'] = list_current_popularity
        df['duration'] = list_waittimes


        data = df[['date', 'hour', 'place_id', 'name', 'lat', 'lng', 'city', 'rating', 'rating_n', 'popularity_monday', 'popularity_tuesday', 'popularity_wednesday', 'popularity_thursday', 'popularity_friday', 'popularity_saturday', 'popularity_sunday', 'live', 'duration']]

        # data.to_csv(output_path + '/' + city + '/' + 'populartimes_' + timestr + '.csv', index=None)
        _converting_and_saving_data(data, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)

        duration = time.time() - start_time

        # Delete the cache
        # shutil.rmtree(dir_path_gpt + timestr) # TODO: Borrar cache
        print(f"Downloaded in {duration} seconds")



    except Exception as e:
        print(e)
        print("Error in extraction_gpt_data")