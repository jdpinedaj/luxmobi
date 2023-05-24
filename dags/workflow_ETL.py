#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

# from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from scripts.extract_transform_data import (
    test_webdriver,
    extraction_bike_data,
    extraction_charging_station_data,
    extraction_traffic_counter_data,
    extraction_parking_data,
)
from scripts.load_data import insert_data_to_table

import os

#######################
##! 2. Default arguments
#######################

default_args = {
    "owner": "juan.pineda",
    "depends_on_past": False,
    "email": ["juan.pineda@uni.lu"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

# It is possible to store all those variables as "Variables" within airflow
# SCHEDULE_INTERVAL: To see the format, check https://crontab.guru/
SCHEDULE_INTERVAL = "0 * * * *"
# SCHEDULE_INTERVAL = "@once"
AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
LOCATION_DATA = "/dags/data/"
SUBLOCATION_BIKE_DATA = "bike_data/"
SUBLOCATION_CHARGING_STATION_DATA = "charging_station_data/"
SUBLOCATION_TRAFFIC_COUNTER_DATA = "traffic_counter_data/"
SUBLOCATION_PARKING_DATA = "parking_data/"
POSTGRESS_CONN_ID = "postgres_default"
POSTGRES_ADDRESS = "host.docker.internal"
POSTGRES_PORT = 5432
POSTGRES_USERNAME = "nipi"
POSTGRES_PASSWORD = "MobiLab1"
POSTGRES_DBNAME = "luxmobi"
CHROMEDRIVER_PATH = "/usr/local/bin/chromedriver"
URL_BIKE = "https://api.jcdecaux.com/vls/v1/stations?contract=Luxembourg&apiKey=4507a17cda9135dd36b8ff13d8a4102ab3aa44a0"
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

# * Those values are needed to create the connection to the Postgres database in the airflow UI
# conn = Connection(conn_id='postgres_default',
#                   conn_type='postgres',
#                   host=POSTGRES_ADDRESS,
#                   schema=POSTGRES_DBNAME,
#                   login=POSTGRES_USERNAME,
#                   password=POSTGRES_PASSWORD,
#                   port=POSTGRES_PORT)

#######################
##! 3. Instantiate a DAG
#######################

dag = DAG(
    dag_id="workflow_ETL",
    description="workflow_ETL",
    start_date=datetime(2023, 5, 24, 7, 0, 0),
    schedule_interval=SCHEDULE_INTERVAL,
    concurrency=5,
    max_active_runs=1,
    default_args=default_args,
)

#######################
##! 4. Tasks
#######################

# ? 4.1. Starting pipeline

start_pipeline = DummyOperator(
    task_id="start_pipeline",
    dag=dag,
)

# # Checking if webdriver is working
# check_webdriver = PythonOperator(
#     task_id="check_webdriver",
#     python_callable=test_webdriver,
#     op_kwargs={
#         "url": URL_PARKING_DATA,
#         "chromedriver_path": CHROMEDRIVER_PATH,
#     },
#     dag=dag,
# )


# ? 4.2. Retrieving data

with TaskGroup(
    "extract_transform_data",
    dag=dag,
) as extract_transform_data:
    bike_data = PythonOperator(
        task_id="bike_data",
        python_callable=extraction_bike_data,
        op_kwargs={
            "url": URL_BIKE,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_BIKE_DATA,
            "file_name": "raw_bike_data",
            "columns": [
                "name",
                "date",
                "hour",
                "lat",
                "long",
                "bike_stands",
                "available_bikes",
                "available_bike_stands",
            ],
        },
        dag=dag,
    )

    # charging_station_data = PythonOperator(
    #     task_id="charging_station_data",
    #     python_callable=extraction_charging_station_data,
    #     op_kwargs={
    #         "url": URL_CHARGING_STATION,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_CHARGING_STATION_DATA,
    #         "file_name": "raw_charging_station_data",
    #         "columns" : [
    #     "date",
    #     "hour",
    #     "lat",
    #     "long",
    #     "address",
    #     "occupied",
    #     "available",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_1 = PythonOperator(
    #     task_id="traffic_counter_data_1",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_1,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_1",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_2 = PythonOperator(
    #     task_id="traffic_counter_data_2",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_2,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_2",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_3 = PythonOperator(
    #     task_id="traffic_counter_data_3",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_3,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_3",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_4 = PythonOperator(
    #     task_id="traffic_counter_data_4",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_4,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_4",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_5 = PythonOperator(
    #     task_id="traffic_counter_data_5",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_5,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_5",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    # traffic_counter_data_6 = PythonOperator(
    #     task_id="traffic_counter_data_6",
    #     python_callable=extraction_traffic_counter_data,
    #     op_kwargs={
    #         "url": URL_TRAFFIC_COUNTER_DATA_6,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_6",
    #         "columns": [
    #     "date",
    #     "hour",
    #     "id",
    #     "lat",
    #     "long",
    #     "road",
    #     "direction",
    #     "percentage",
    #     "speed",
    #     "vehicle_flow_rate",
    # ]
    #     },
    #     dag=dag,
    # )

    parking_data = PythonOperator(
        task_id="parking_data",
        python_callable=extraction_parking_data,
        op_kwargs={
            "url": URL_PARKING_DATA,
            "chromedriver_path": CHROMEDRIVER_PATH,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_PARKING_DATA,
            "file_name": "raw_parking_data",
            "columns": [
                "date",
                "hour",
                "name",
                "available",
                "total",
                "occupancy",
                "trend",
            ],
        },
        dag=dag,
    )

# ? 4.3. Writing data to database

with TaskGroup(
    "load_data",
    dag=dag,
) as load_data:
    load_bike_data = PythonOperator(
        task_id="load_bike_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_BIKE_DATA,
            "file_name": "raw_bike_data",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "bike",
            "columns_table": "name, date, hour, lat, long, total_bike_stand, bike_available, bike_stands_available",
            "data_type": "bike",
        },
        dag=dag,
    )

    # load_charging_station_data = PythonOperator(
    #     task_id="load_charging_station_data",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_CHARGING_STATION_DATA,
    #         "file_name": "raw_charging_station_data",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "charging_station",
    #         "columns_table":
    #         "date, hour, lat, long, address, occupied, available",
    #         "data_type": "charging_station"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_1 = PythonOperator(
    #     task_id="load_traffic_counter_data_1",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_1",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_2 = PythonOperator(
    #     task_id="load_traffic_counter_data_2",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_2",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_3 = PythonOperator(
    #     task_id="load_traffic_counter_data_3",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_3",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_4 = PythonOperator(
    #     task_id="load_traffic_counter_data_4",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_4",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_5 = PythonOperator(
    #     task_id="load_traffic_counter_data_5",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_5",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    # load_traffic_counter_data_6 = PythonOperator(
    #     task_id="load_traffic_counter_data_6",
    #     python_callable=insert_data_to_table,
    #     op_kwargs={
    #         "postgres_conn_id": POSTGRESS_CONN_ID,
    #         "airflow_home": AIRFLOW_HOME,
    #         "location_data": LOCATION_DATA,
    #         "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
    #         "file_name": "raw_traffic_counter_data_6",
    #         "db_name": POSTGRES_DBNAME,
    #         "schema_name": "raw",
    #         "table_name": "traffic_counter",
    #         "columns_table":
    #         "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
    #         "data_type": "traffic_counter"
    #     },
    #     dag=dag,
    # )

    load_parking_data = PythonOperator(
        task_id="load_parking_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_PARKING_DATA,
            "file_name": "raw_parking_data",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "parking",
            "columns_table": "date, hour, name, available, total, occupancy, trend",
            "data_type": "parking",
        },
        dag=dag,
    )

    bike_data >> load_bike_data
    # charging_station_data >> load_charging_station_data
    # traffic_counter_data_1 >> load_traffic_counter_data_1
    # traffic_counter_data_2 >> load_traffic_counter_data_2
    # traffic_counter_data_3 >> load_traffic_counter_data_3
    # traffic_counter_data_4 >> load_traffic_counter_data_4
    # traffic_counter_data_5 >> load_traffic_counter_data_5
    # traffic_counter_data_6 >> load_traffic_counter_data_6
    parking_data >> load_parking_data

# ? 4.4. Finishing pipeline

finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

# start_pipeline >> check_webdriver >> extract_transform_data
start_pipeline >> extract_transform_data

extract_transform_data >> load_data

load_data >> finish_pipeline
