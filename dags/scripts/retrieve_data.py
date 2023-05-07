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


def retrieve_bike_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
) -> None:
    """
    This function retrieves data from the JCDecaux API and saves it in a csv file.
    Args:
        url (str): url of the JCDecaux API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
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
        bike_dtands = el["bike_stands"]
        available_bikes = el["available_bikes"]
        available_bike_stands = el["available_bike_stands"]

        row.append(name)
        row.append(date)
        row.append(hour)
        row.append(lat)
        row.append(long)
        row.append(bike_dtands)
        row.append(available_bikes)
        row.append(available_bike_stands)
        rows.append(row)

    _converting_and_saving_data(rows, [
        "name",
        "date",
        "hour",
        "lat",
        "long",
        "bike_stands",
        "available_bikes",
        "available_bike_stands",
    ], airflow_home, location_data, sublocation_data, file_name, date, hour)


def retrieve_charging_station_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
) -> None:
    """
    This function retrieves data from the Public.lu API and saves it in a csv file.
    Args:
        url (str): url of the Public.lu API
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
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

    _converting_and_saving_data(rows, [
        "date",
        "hour",
        "lat",
        "long",
        "address",
        "occupied",
        "available",
    ], airflow_home, location_data, sublocation_data, file_name, date, hour)


def retrieve_traffic_counter_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
) -> None:
    """
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

    _converting_and_saving_data(rows, [
        "date",
        "hour",
        "id",
        "lat",
        "long",
        "road",
        "direction",
        "percentage",
        "speed",
        "vehicle_flow_rate",
    ], airflow_home, location_data, sublocation_data, file_name, date, hour)


def retrieve_parking_data(
    url: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
) -> None:
    """
    """

    date, hour = _now_times()

    chrome_options = Options()
    prefs = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome("/usr/bin/chromedriver", options=chrome_options)

    ##################
    #! Esto esta en un try

    driver.get(url)
    time.sleep(2)
    urlElems = driver.find_elements_by_xpath("//div[@class='panel-header']")

    rows = []
    for father in urlElems:
        row = []
        row.append(date)
        row.append(hour)
        name = father.find_element_by_xpath(
            ".//span[@class='h6 panel-main-title']")
        row.append(name.text)
        b = father.find_element_by_xpath(".//div[@class='parking-data']")
        try:
            if (b.text == 'No data available'):
                row.append('None')
                row.append('None')
                row.append('None')
                row.append('None')
                rows.append(row)
                continue
            p = b.text.split()
            available = p[0]
            row.append(available)
            total = p[-1].replace('/', '')
            row.append(total)
            #print(p[0],p[-1])
            row.append(100 - ((float(available) / float(total)) * 100))

            ac = father.find_element_by_xpath(
                ".//*[local-name() = 'svg' and @width='12']")
            row.append(ac.get_attribute('class'))

            rows.append(row)

        except:
            row.append('None')
            row.append('None')
            row.append('None')
            row.append('None')

            rows.append(row)

    _converting_and_saving_data(rows, [
        "date",
        "hour",
        "name",
        "available",
        "total",
        "occupancy",
        "trend",
    ], airflow_home, location_data, sublocation_data, file_name, date, hour)

    driver.close()
