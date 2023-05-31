import requests
import json
from xml.dom import minidom
import urllib.request as ur
from datetime import datetime
import pandas as pd
# from dags.scripts.utils.logs import logger

# Libraries for web scraping

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException

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


def extraction_gpt_data(url: str, chromedriver_path: str, airflow_home: str, location_data: str, sublocation_data: str, file_name: str,
                          columns: list) -> None:
    """
    This function extracts data from the Google Popular Times website and saves it in a csv file.
    Args:
        url (str): url of the Google Popular Times website
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
            
            try:
                row.append('city')
                row.append('id')
                row.append('rating')
                row.append('rating_n')
                row.append('popularity')
                row.append('live')
                row.append('duration')

                rows.append(row)

            except Exception as e:
                row.extend(['Error']*7)
                rows.append(row)
        
        _converting_and_saving_data(rows, columns, airflow_home, location_data, sublocation_data, file_name, date, hour)

    finally:
        driver.quit()

