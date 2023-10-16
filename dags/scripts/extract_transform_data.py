from typing import Any, Tuple
import requests
import json
from xml.dom import minidom
import urllib.request as ur
from datetime import datetime
import pandas as pd
import os
import json

from .utils.logs import logger

# Libraries for web scraping
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# For gpt data
import re
import calendar
import shutil
import asyncio
import aiohttp
import aiofiles
import scripts.config as config
import scripts.gpt_locations as gpt_locations

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
    try:
        now = datetime.now()
        date = now.strftime("%Y%m%d")
        hour = now.strftime("%H")
        logger.info(f"Date: {date}, Hour: {hour}")

        return date, hour

    except Exception as e:
        logger.error(f"An error occurred in _now_times: {str(e)}")
        raise


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
    try:
        df = pd.DataFrame(data, columns=columns)

        dir_path = airflow_home + location_data + sublocation_data
        file_path = dir_path + date + "_" + hour + "_" + file_name + ".csv"

        df.to_csv(file_path, index=False)
        logger.info(f"Data saved in {file_path}")

    except Exception as e:
        logger.error(f"An error occurred in _converting_and_saving_data: {str(e)}")
        raise


async def _download_site(
    session: aiohttp.ClientSession,
    url: str,
    place: str,
    timestr: str,
    count: int,
    airflow_home: str,
    location_data: str,
    sublocation_cache: str,
) -> str:
    """
    This function downloads the data from the url.
    Args:
        session (aiohttp.ClientSession): session
        url (str): url to download
        place (str): place to download
        timestr (str): time of the download
        count (int): count of the download
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_cache (str): path to sublocation cache
    Returns:
        filename (str): name of the file
    """
    try:
        response = await session.request(method="GET", url=url, ssl=config.GCONTEXT)
        if response.status != 200:
            print(f"Failed to download {url}, status code: {response.status}")
            return
        filename = (
            airflow_home
            + location_data
            + sublocation_cache
            + config.CITY
            + "/"
            + timestr
            + "/"
            + str(place)
            + ".txt"
        )
        print(count, place)

        # Ensure directory exists before writing to it
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        async with aiofiles.open(filename, "ba") as f:
            async for content in response.content.iter_chunked(1024):
                await f.write(content)

        logger.info(f"Downloaded {url} to {filename}")
        return filename

    except Exception as e:
        logger.error(f"An error occurred in _download_site: {str(e)}")
        raise


async def _download_all_sites(
    timestr: str,
    places: list,
    airflow_home: str,
    location_data: str,
    sublocation_cache: str,
) -> None:
    """
    This function downloads all the sites, using _download_site function.
    Args:
        timestr (str): time of the download
        places (list): list of places to download
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_cache (str): path to sublocation cache
    Returns:
        None
    """
    try:
        timeout = aiohttp.ClientTimeout(total=60 * 60)
        connector = aiohttp.TCPConnector(limit=20)
        async with aiohttp.ClientSession(
            connector=connector,
            headers=config.USER_AGENT,
            cookies=config.COOKIES,
            timeout=timeout,
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
                    _download_site(
                        session,
                        search_url,
                        place_identifier[2],
                        timestr,
                        count,
                        airflow_home,
                        location_data,
                        sublocation_cache,
                    )
                )
                tasks.append(task_asyn)

            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("All sites downloaded")

    except Exception as e:
        logger.error(f"An error occurred in _download_all_sites: {str(e)}")
        raise


def _get_populartimes_from_search(
    place_identifier: str, path: str
) -> Tuple[float, int, dict, dict]:
    """
    This function gets the popular times from the search, using the sites downloaded in _download_all_sites function.
    It is based on https://github.com/m-wrzr/populartimes
    Args:
        place_identifier (str): place identifier
        path (str): path to the sites
    Returns:
        rating (float): rating of the place
        rating_n (int): number of ratings of the place
        popular_times (dict): popular times of the place
        time_spent (dict): time spent of the place
    """
    try:
        with open(path + "/" + str(place_identifier) + ".txt", mode="rb") as d:
            content = d.read()

        data = content.decode("utf-8").split('/*""*/')[0]
        logger.info(f"Data retrieved from {path}/{str(place_identifier)}.txt")

    except Exception as e:
        logger.error(
            f"An error occurred when retrieving data from {path}/{str(place_identifier)}.txt, in _get_populartimes_from_search: {str(e)}"
        )
        raise

    try:
        idx1 = data.index("APP_INITIALIZATION_STATE=")
        data_new = data[idx1 + 25 :]
        idx2 = data_new.index(";window.APP_FLAGS=")

        final_string = data_new[:idx2]

        d = final_string.split("""'\\n""")
        final_string = d[2].replace("\\\\\\", "#?replace?#")
        final_string = final_string.replace("\\", "")
        final_string = final_string.replace("#?replace?#", "\\")
        final_string = "[[" + final_string.replace(']"]', "]]")
        flag_error = True
        count_error = 0
        while flag_error and count_error < 250:
            count_error += 1
            try:
                jdata = json.loads(r"" + final_string)
                flag_error = False
            except Exception as e:
                msg = e.msg
                if "Expecting ',' delimiter" in e.msg:
                    final_string = (
                        final_string[: e.pos - 1] + "" + final_string[e.pos :]
                    )
                else:
                    final_string = (
                        final_string[: e.pos] + "" + final_string[e.pos + 1 :]
                    )

        if count_error == 250:
            raise Exception(
                "Error from load Json "
                + msg
                + "  https://www.google.com/maps/place/?q=place_id:"
                + place_identifier
            )
        info = _index_get(jdata, 0, 0, 6)

        rating = _index_get(info, 4, 7)
        rating_n = _index_get(info, 4, 8)

        popular_times = _index_get(info, 84, 0)

        # current_popularity is also not available if popular_times isn't
        current_popularity = _index_get(info, 84, 7, 1)

        time_spent = _index_get(info, 117, 0)

        # extract wait times and convert to minutes
        if time_spent:
            nums = [
                float(f)
                for f in re.findall(r"\d*\.\d+|\d+", time_spent.replace(",", "."))
            ]
            contains_min, contains_hour = (
                "min" in time_spent,
                "hour" in time_spent or "hr" in time_spent,
            )

            time_spent = None

            if contains_min and contains_hour:
                time_spent = [nums[0], nums[1] * 60]
            elif contains_hour:
                time_spent = [
                    nums[0] * 60,
                    (nums[0] if len(nums) == 1 else nums[1]) * 60,
                ]
            elif contains_min:
                time_spent = [nums[0], nums[0] if len(nums) == 1 else nums[1]]

            time_spent = [int(t) for t in time_spent]

        logger.info(f"Data retrieved in _get_populartimes_from_search")
        return rating, rating_n, popular_times, current_popularity, time_spent
    except Exception as e:
        logger.error(f"An error occurred in _get_populartimes_from_search: {str(e)}")
        raise
    # return  None,None,None,None,None


def _get_popularity_for_day(popularity: list) -> Tuple[list, list]:
    """
    This function gets the popularity for each day of the week.
    It is based on https://github.com/m-wrzr/populartimes
    Args:
        popularity (list): list of popularity
    Returns:
        ret_popularity (list): list of popularity for each day of the week
        ret_wait (list): list of waiting time for each day of the week
    """
    try:
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
                        wait_digits = re.findall(r"\d+", hour_info[3])

                        if len(wait_digits) == 0:
                            wait_json[day_no - 1][hour] = 0
                        elif "min" in hour_info[3]:
                            wait_json[day_no - 1][hour] = int(wait_digits[0])
                        elif "hour" in hour_info[3]:
                            wait_json[day_no - 1][hour] = int(wait_digits[0]) * 60
                        else:
                            wait_json[day_no - 1][hour] = int(
                                wait_digits[0]
                            ) * 60 + int(wait_digits[1])

                    # day wrap
                    if hour_info[0] == 23:
                        day_no = day_no % 7 + 1

        ret_popularity = [
            {"name": list(calendar.day_name)[d], "data": pop_json[d]} for d in range(7)
        ]

        ret_popularity_monday = ret_popularity[0]["data"]
        ret_popularity_tuesday = ret_popularity[1]["data"]
        ret_popularity_wednesday = ret_popularity[2]["data"]
        ret_popularity_thursday = ret_popularity[3]["data"]
        ret_popularity_friday = ret_popularity[4]["data"]
        ret_popularity_saturday = ret_popularity[5]["data"]
        ret_popularity_sunday = ret_popularity[6]["data"]

        # waiting time only if applicable
        ret_wait = (
            [
                {"name": list(calendar.day_name)[d], "data": wait_json[d]}
                for d in range(7)
            ]
            if any(any(day) for day in wait_json)
            else []
        )

        # {"name" : "monday", "data": [...]} for each weekday as list

        logger.info(f"Data retrieved in _get_popularity_for_day")
        return (
            ret_popularity_monday,
            ret_popularity_tuesday,
            ret_popularity_wednesday,
            ret_popularity_thursday,
            ret_popularity_friday,
            ret_popularity_saturday,
            ret_popularity_sunday,
        )

    except Exception as e:
        logger.error(f"An error occurred in _get_popularity_for_day: {str(e)}")
        raise


def _index_get(array: list, *argv: int) -> Any:
    """
    This function checks if a index is available in the array and returns it.
    If the index is out of range or the value is not available, it returns None.
    It is based on https://github.com/m-wrzr/populartimes

    Args:
        array (list): array to get the value from
        *argv (int): index of the value
    Returns:
        value (any): value of the index
    """
    try:
        for index in argv:
            array = array[index]
        logger.info(f"Data retrieved in _index_get")
        return array

    # there is either no info available or no popular times
    # TypeError: rating/rating_n/populartimes wrong of not available
    except (IndexError, TypeError):
        logger.info(
            f"IndexError or TypeError in _index_get, returning None in _index_get"
        )
        return None


#######################


def test_webdriver(url: str, chromedriver_path: str):
    """
    This function tests the webdriver.
    Args:
        url (str): url to test
        chromedriver_path (str): path to chromedriver
    Returns:
        None
    """
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=DEBUG")
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(
            executable_path=chromedriver_path, options=chrome_options
        )
        logger.info(f"Webdriver started in test_webdriver")
    except Exception as e:
        logger.error(f"An error occurred in test_webdriver: {str(e)}")
        raise
    try:
        driver.get(url)
        time.sleep(2)
        logger.info(
            f"Data retrieved in test_webdriver, and driver title is {driver.title}"
        )
    except Exception as e:
        logger.error(f"An error occurred in test_webdriver: {str(e)}")
        raise

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
    try:
        date, hour = _now_times()
        response = requests.get(url)
        data = json.loads(response.text)

        rows = []
        for el in data:
            row = []
            name = el["name"].replace("'", "")
            latitude = el["position"]["lat"]
            longitude = el["position"]["lng"]
            bike_stands = el["bike_stands"]
            available_bikes = el["available_bikes"]
            available_bike_stands = el["available_bike_stands"]

            row.append(name)
            row.append(date)
            row.append(hour)
            row.append(latitude)
            row.append(longitude)
            row.append(bike_stands)
            row.append(available_bikes)
            row.append(available_bike_stands)
            rows.append(row)

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )
        logger.info(f"Data retrieved in extraction_bike_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_bike_data: {str(e)}")
        raise


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
    try:
        date, hour = _now_times()

        dom = minidom.parse(ur.urlopen(url))
        owned = dom.getElementsByTagName("Placemark")

        rows = []
        for el in owned:
            row = []

            latitude = el.getElementsByTagName("coordinates")[0].firstChild.data.split(
                ","
            )[1]
            longitude = el.getElementsByTagName("coordinates")[0].firstChild.data.split(
                ","
            )[0]
            address = (
                el.getElementsByTagName("address")[0]
                .firstChild.nodeValue.replace(",", "")
                .replace("'", "")
            )
            occupied = (
                el.getElementsByTagName("description")[0]
                .firstChild.nodeValue.split("occupied connectors")[0]
                .split("</b>")[-2]
                .split("<b>")[-1]
            )
            available = (
                el.getElementsByTagName("description")[0]
                .firstChild.nodeValue.split("available connectors")[0]
                .split("</b>")[-2]
                .split("<b>")[-1]
            )

            row.append(date)
            row.append(hour)
            row.append(latitude)
            row.append(longitude)
            row.append(address)
            row.append(occupied)
            row.append(available)
            rows.append(row)

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )
        logger.info(f"Data retrieved in extraction_charging_station_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_charging_station_data: {str(e)}")
        raise


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
    try:
        date, hour = _now_times()

        dom = minidom.parse(ur.urlopen(url))
        owned = dom.getElementsByTagName("siteMeasurements")

        rows = []
        for el in owned:
            row = []

            id = el.getElementsByTagName("measurementSiteReference")[0].getAttribute(
                "id"
            )
            latitude = el.getElementsByTagName("latitude")[0].firstChild.nodeValue
            longitude = el.getElementsByTagName("longitude")[0].firstChild.nodeValue
            road = el.getElementsByTagName("roadNumber")[0].firstChild.nodeValue
            direction_tags = dom.getElementsByTagName("directionBoundOnLinearSection")
            if direction_tags:
                direction = direction_tags[0].firstChild.nodeValue
            else:
                direction = None

            percentage = el.getElementsByTagName("percentage")[0].firstChild.nodeValue
            speed = el.getElementsByTagName("speed")[0].firstChild.nodeValue
            vehicle_flow_rate = el.getElementsByTagName("vehicleFlowRate")[
                0
            ].firstChild.nodeValue

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

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )

        logger.info(f"Data retrieved in extraction_traffic_counter_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_traffic_counter_data: {str(e)}")
        raise


def extraction_stops_public_transport_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
    """
    This function extracts data from the stops public transport API and saves it in a csv file.
    Args:
        url (str): url of the stops public transport API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    try:
        date, hour = _now_times()
        response = requests.get(url)
        data = json.loads(response.text)

        columns = [
            "date",
            "hour",
            "name",
            "line",
            "cat_out",
            "cls",
            "cat_out_s",
            "cat_out_l",
            "extid",
            "bus_stop",
            "latitude",
            "longitude",
            "weight",
            "dist",
            "products",
        ]
        rows = []
        for location in data["stopLocationOrCoordLocation"]:
            stop_location = location.get("StopLocation", {})
            if "productAtStop" in stop_location:
                for product in stop_location["productAtStop"]:
                    row_data = {
                        "date": date,
                        "hour": hour,
                        "name": product.get("name", None)
                        .replace(",", "")
                        .replace("'", ""),
                        "line": product.get("line", None),
                        "cat_out": product.get("catOut", None),
                        "cls": product.get("cls", None),
                        "cat_out_s": product.get("catOutS", None),
                        "cat_out_l": product.get("catOutL", None),
                        "extid": stop_location.get("extId", None),
                        "bus_stop": stop_location.get("name", "")
                        .replace(",", "")
                        .replace("'", ""),
                        "latitude": stop_location.get("lat", None),
                        "longitude": stop_location.get("lon", None),
                        "weight": stop_location.get("weight", None),
                        "dist": stop_location.get("dist", None),
                        "products": stop_location.get("products", None),
                    }
                    rows.append([row_data[col] for col in columns])

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )
        logger.info(f"Data retrieved in extraction_stops_public_transport_data")

    except Exception as e:
        logger.error(
            f"An error occurred in extraction_stops_public_transport_data: {str(e)}"
        )
        raise


def extraction_departure_board_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
    """
    This function extracts data from the departure board API and saves it in a csv file.
    Args:
        url (str): url of the departure board API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        columns (list): list of the columns
    Returns:
        None
    """
    try:
        date, hour = _now_times()
        response = requests.get(url)
        data = json.loads(response.text)

        rows = []

        # Check if there are departures before processing
        if not data.get("Departure", None):
            logger.warning(
                f"No departures found in the received data, using extraction_departure_board_data method"
            )
            return

        for departure in data["Departure"]:
            product = departure["Product"]
            row = []
            name = product["name"]
            num = product["num"]
            line = product["line"]
            cat_out = product["catOut"]
            cat_in = product["catIn"]
            cat_code = product["catCode"]
            cls = product["cls"]
            operator_code = product["operatorCode"]
            operator = product["operator"].replace(",", "")

            bus_name = departure["name"]
            type = departure["type"]
            stop = departure["stop"].replace(",", "")
            stop_ext_id = departure["stopExtId"]
            direction = departure["direction"].replace(",", "")
            train_number = departure["trainNumber"]
            train_category = departure["trainCategory"]

            row.append(date)
            row.append(hour)
            row.append(name)
            row.append(num)
            row.append(line)
            row.append(cat_out)
            row.append(cat_in)
            row.append(cat_code)
            row.append(cls)
            row.append(operator_code)
            row.append(operator)
            row.append(bus_name)
            row.append(type)
            row.append(stop)
            row.append(stop_ext_id)
            row.append(direction)
            row.append(train_number)
            row.append(train_category)

            rows.append(row)

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )

        logger.info(f"Data retrieved in extraction_departure_board_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_departure_board_data: {str(e)}")
        raise


def extraction_parking_data(
    url: str,
    chromedriver_path: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
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
    try:
        date, hour = _now_times()

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--log-level=DEBUG")
        prefs = {"profile.managed_default_content_settings.images": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(
            executable_path=chromedriver_path, options=chrome_options
        )

        logger.info(f"Extracting data from {url}")

    except Exception as e:
        logger.error(
            f"An error occurred when opening the browser in extraction_parking_data: {str(e)}"
        )
        raise

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
                if b.text == "No data available":
                    row.extend([-1] * 3)
                    row.extend(["No data available"])
                    rows.append(row)
                elif b.text == "Closed":
                    row.extend([-1] * 3)
                    row.extend(["Closed"])
                    rows.append(row)
                else:
                    p = b.text.split()
                    available = p[0]
                    total = p[-1].replace("/", "")
                    occupancy = 100 - ((float(available) / float(total)) * 100)
                    occupancy = round(occupancy, 2)
                    ac = father.find_element(
                        "xpath", ".//*[local-name() = 'svg' and @width='12']"
                    )

                    row.append(available)
                    row.append(total)
                    row.append(occupancy)
                    row.append(ac.get_attribute("class"))

                    rows.append(row)

            except Exception as e:
                row.extend([-1] * 3)
                row.extend(["Error"])
                rows.append(row)

        _converting_and_saving_data(
            rows,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )
        logger.info(f"Data retrieved in extraction_parking_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_parking_data: {str(e)}")
        raise

    finally:
        driver.quit()


def extraction_gpt_data(
    airflow_home: str,
    location_data: str,
    sublocation_cache: str,
    sublocation_data: str,
    file_name: str,
    columns: list,
) -> None:
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
        dir_path_gpt = (
            airflow_home + location_data + sublocation_cache + config.CITY + "/"
        )
        # dir_path_gpt = raw_data_path + config.CITY + "/"
        try:
            os.mkdir(dir_path_gpt[:-1])
        except FileExistsError:
            pass
        try:
            os.mkdir(dir_path_gpt + timestr)
        except FileExistsError:
            pass

        # input file with list of POIs with names, house number, street, etc

        df = pd.DataFrame(gpt_locations.DICT_GPT_LOCATIONS)

        # Removing ' to column 'name'
        df["name"] = df["name"].str.replace("'", "")

        list_places = []
        place_ids_list = list(df["place_id"])
        ind_list = list(df["index"])

        for i in range(len(place_ids_list)):
            place = place_ids_list[i]
            list_places.append([place, True, ind_list[i]])

        # Download data
        asyncio.run(
            _download_all_sites(
                timestr, list_places, airflow_home, location_data, sublocation_cache
            )
        )

        # Extract needed data
        list_rating = []
        list_rating_n = []
        list_current_popularity = []
        list_waittimes = []
        list_popularity = []

        for i in range(len(list_places)):
            try:
                (
                    rating,
                    rating_n,
                    popular_times,
                    current_popularity,
                    time_spent,
                ) = _get_populartimes_from_search(
                    list_places[i][2], dir_path_gpt + timestr
                )
                (
                    ret_popularity_monday,
                    ret_popularity_tuesday,
                    ret_popularity_wednesday,
                    ret_popularity_thursday,
                    ret_popularity_friday,
                    ret_popularity_saturday,
                    ret_popularity_sunday,
                ) = _get_popularity_for_day(popular_times)

                list_popularity.append(
                    {
                        "popularity_monday": ret_popularity_monday,
                        "popularity_tuesday": ret_popularity_tuesday,
                        "popularity_wednesday": ret_popularity_wednesday,
                        "popularity_thursday": ret_popularity_thursday,
                        "popularity_friday": ret_popularity_friday,
                        "popularity_saturday": ret_popularity_saturday,
                        "popularity_sunday": ret_popularity_sunday,
                    }
                )
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
                list_popularity.append(
                    {
                        "popularity_monday": "None",
                        "popularity_tuesday": "None",
                        "popularity_wednesday": "None",
                        "popularity_thursday": "None",
                        "popularity_friday": "None",
                        "popularity_saturday": "None",
                        "popularity_sunday": "None",
                    }
                )
                list_rating.append("None")
                list_rating_n.append("None")
                list_current_popularity.append("None")
                list_waittimes.append("None")

        # Create a dataframe with the data
        df["date"] = date
        df["hour"] = hour
        df["city"] = config.CITY
        df["rating"] = list_rating
        df["rating_n"] = list_rating_n
        df["popularity_monday"] = [
            str(popularity.get("popularity_monday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_tuesday"] = [
            str(popularity.get("popularity_tuesday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_wednesday"] = [
            str(popularity.get("popularity_wednesday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_thursday"] = [
            str(popularity.get("popularity_thursday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_friday"] = [
            str(popularity.get("popularity_friday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_saturday"] = [
            str(popularity.get("popularity_saturday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["popularity_sunday"] = [
            str(popularity.get("popularity_sunday", [])).replace(",", "")
            for popularity in list_popularity
        ]
        df["live"] = list_current_popularity
        df["duration"] = list_waittimes

        # Select the columns to be saved
        data = df[
            [
                "date",
                "hour",
                "place_id",
                "name",
                "latitude",
                "longitude",
                "city",
                "rating",
                "rating_n",
                "popularity_monday",
                "popularity_tuesday",
                "popularity_wednesday",
                "popularity_thursday",
                "popularity_friday",
                "popularity_saturday",
                "popularity_sunday",
                "live",
                "duration",
            ]
        ]

        # Save the data
        _converting_and_saving_data(
            data,
            columns,
            airflow_home,
            location_data,
            sublocation_data,
            file_name,
            date,
            hour,
        )

        # Delete the cache
        shutil.rmtree(dir_path_gpt + timestr)

        logger.info(f"Data retrieved in extraction_gpt_data")

    except Exception as e:
        logger.error(f"An error occurred in extraction_gpt_data: {str(e)}")
        raise
