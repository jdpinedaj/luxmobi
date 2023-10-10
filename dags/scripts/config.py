import os
import ssl

#! PARAMETERS

# SCHEDULE_INTERVAL: To see the format, check https://crontab.guru/
SCHEDULE_INTERVAL_HOURLY = "0 * * * *"
SCHEDULE_INTERVAL_ONCE = "@once"
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

# Location of the data
LOCATION_DATA = "/dags/data/"
SUBLOCATION_BIKE_DATA = "bike_data/"
SUBLOCATION_STOPS_PUBLIC_TRANSPORT_DATA = "stops_public_transport_data/"
SUBLOCATION_DEPARTURE_BOARD_DATA = "departure_board_data/"
SUBLOCATION_CHARGING_STATION_DATA = "charging_station_data/"
SUBLOCATION_TRAFFIC_COUNTER_DATA = "traffic_counter_data/"
SUBLOCATION_PARKING_DATA = "parking_data/"
SUBLOCATION_GPT_DATA = "gpt_data/"
SUBLOCATION_GPT_DATA_CACHE = "gpt_data/cache/"

# MysQL connection
MYSQL_CONN_ID = "mysql_default"
MYSQL_ADDRESS = "10.186.32.20"
MYSQL_PORT = 3306
MYSQL_USERNAME = "nipi"
MYSQL_PASSWORD = "MobiL@b1"
MYSQL_DBNAME = "luxmob"

# Postgres connection
POSTGRESS_CONN_ID = "postgres_default"
POSTGRES_ADDRESS = "host.docker.internal"
POSTGRES_PORT = 5432
POSTGRES_USERNAME = "nipi"
POSTGRES_PASSWORD = "MobiLab1"
POSTGRES_DBNAME = "luxmobi"

# Web scraping
CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"
URL_BIKE = "https://api.jcdecaux.com/vls/v1/stations?contract=Luxembourg&apiKey=4507a17cda9135dd36b8ff13d8a4102ab3aa44a0"
URL_STOPS_PUBLIC_TRANSPORT = "https://cdt.hafas.de/opendata/apiserver/location.nearbystops?accessId=5287a4f9-5003-4c6c-9282-ff67e7eed013&originCoordLong=6.09528&originCoordLat=49.77723&maxNo=5000&r=100000&format=json"
URL_DEPARTURE_BOARD = "https://cdt.hafas.de/opendata/apiserver/departureBoard?accessId=5287a4f9-5003-4c6c-9282-ff67e7eed013&lang=fr&id=200426002&format=json"
URL_CHARGING_STATION = (
    "https://data.public.lu/en/datasets/r/22f9d77a-5138-4b02-b315-15f306b77034"
)
URL_PARKING_DATA = "https://www.vdl.lu/en/getting-around/by-car/parkings-and-pr"
URL_TRAFFIC_COUNTER_DATA_1 = "https://www.cita.lu/info_trafic/datex/trafficstatus_b40"
URL_TRAFFIC_COUNTER_DATA_2 = "http://www.cita.lu/info_trafic/datex/trafficstatus_a13"
URL_TRAFFIC_COUNTER_DATA_3 = "http://www.cita.lu/info_trafic/datex/trafficstatus_a7"
URL_TRAFFIC_COUNTER_DATA_4 = "http://www.cita.lu/info_trafic/datex/trafficstatus_a6"
URL_TRAFFIC_COUNTER_DATA_5 = "https://www.cita.lu/info_trafic/datex/trafficstatus_a4"
URL_TRAFFIC_COUNTER_DATA_6 = "https://www.cita.lu/info_trafic/datex/trafficstatus_a3"

# Params for Google Popular Times
USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15"
}
COOKIES = {"CONSENT": "YES+cb.20210720-07-p0.en+FX+410"}
CITY = "Luxembourg"
GCONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS)
