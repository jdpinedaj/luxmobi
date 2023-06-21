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
    extraction_gpt_data,
)
from scripts.load_data import insert_data_to_table
import scripts.params as params
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

SCHEDULE_INTERVAL = params.SCHEDULE_INTERVAL_ONCE
AIRFLOW_HOME = params.AIRFLOW_HOME
LOCATION_DATA = params.LOCATION_DATA
SUBLOCATION_BIKE_DATA = params.SUBLOCATION_BIKE_DATA
SUBLOCATION_CHARGING_STATION_DATA = params.SUBLOCATION_CHARGING_STATION_DATA
SUBLOCATION_TRAFFIC_COUNTER_DATA = params.SUBLOCATION_TRAFFIC_COUNTER_DATA
SUBLOCATION_PARKING_DATA = params.SUBLOCATION_PARKING_DATA
SUBLOCATION_GPT_DATA = params.SUBLOCATION_GPT_DATA
SUBLOCATION_GPT_DATA_CACHE = params.SUBLOCATION_GPT_DATA_CACHE
POSTGRESS_CONN_ID = params.POSTGRESS_CONN_ID
POSTGRES_ADDRESS = params.POSTGRES_ADDRESS
POSTGRES_PORT = params.POSTGRES_PORT
POSTGRES_USERNAME = params.POSTGRES_USERNAME
POSTGRES_PASSWORD = params.POSTGRES_PASSWORD
POSTGRES_DBNAME = params.POSTGRES_DBNAME
CHROMEDRIVER_PATH = params.CHROMEDRIVER_PATH
URL_BIKE = params.URL_BIKE
URL_CHARGING_STATION = params.URL_CHARGING_STATION
URL_PARKING_DATA = params.URL_PARKING_DATA
URL_TRAFFIC_COUNTER_DATA_1 = params.URL_TRAFFIC_COUNTER_DATA_1
URL_TRAFFIC_COUNTER_DATA_2 = params.URL_TRAFFIC_COUNTER_DATA_2
URL_TRAFFIC_COUNTER_DATA_3 = params.URL_TRAFFIC_COUNTER_DATA_3
URL_TRAFFIC_COUNTER_DATA_4 = params.URL_TRAFFIC_COUNTER_DATA_4
URL_TRAFFIC_COUNTER_DATA_5 = params.URL_TRAFFIC_COUNTER_DATA_5
URL_TRAFFIC_COUNTER_DATA_6 = params.URL_TRAFFIC_COUNTER_DATA_6


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


# ? 4.2. Extracting and Transforming data

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

    charging_station_data = PythonOperator(
        task_id="charging_station_data",
        python_callable=extraction_charging_station_data,
        op_kwargs={
            "url": URL_CHARGING_STATION,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_CHARGING_STATION_DATA,
            "file_name": "raw_charging_station_data",
            "columns" : [
        "date",
        "hour",
        "lat",
        "long",
        "address",
        "occupied",
        "available",
    ]
        },
        dag=dag,
    )

    traffic_counter_data_1 = PythonOperator(
        task_id="traffic_counter_data_1",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_1,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_1",
            "columns": [
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
    ]
        },
        dag=dag,
    )

    traffic_counter_data_2 = PythonOperator(
        task_id="traffic_counter_data_2",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_2,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_2",
            "columns": [
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
    ]
        },
        dag=dag,
    )

    traffic_counter_data_3 = PythonOperator(
        task_id="traffic_counter_data_3",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_3,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_3",
            "columns": [
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
    ]
        },
        dag=dag,
    )

    traffic_counter_data_4 = PythonOperator(
        task_id="traffic_counter_data_4",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_4,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_4",
            "columns": [
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
    ]
        },
        dag=dag,
    )

    traffic_counter_data_5 = PythonOperator(
        task_id="traffic_counter_data_5",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_5,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_5",
            "columns": [
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
    ]
        },
        dag=dag,
    )

    traffic_counter_data_6 = PythonOperator(
        task_id="traffic_counter_data_6",
        python_callable=extraction_traffic_counter_data,
        op_kwargs={
            "url": URL_TRAFFIC_COUNTER_DATA_6,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_6",
            "columns": [
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
    ]
        },
        dag=dag,
    )

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

    gpt_data = PythonOperator(
        task_id="gpt_data",
        python_callable=extraction_gpt_data,
        op_kwargs={
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_cache": SUBLOCATION_GPT_DATA_CACHE,
            "sublocation_data": SUBLOCATION_GPT_DATA,
            "file_name": "raw_gpt_data",
            "columns": [
                "date",
                "hour",
                "place_id",
                "name",
                "lat",
                "long",
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
            ],
        },
        dag=dag,
    )


# ? 4.3. Loading data

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

    load_charging_station_data = PythonOperator(
        task_id="load_charging_station_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_CHARGING_STATION_DATA,
            "file_name": "raw_charging_station_data",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "charging_station",
            "columns_table":
            "date, hour, lat, long, address, occupied, available",
            "data_type": "charging_station"
        },
        dag=dag,
    )

    load_traffic_counter_data_1 = PythonOperator(
        task_id="load_traffic_counter_data_1",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_1",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

    load_traffic_counter_data_2 = PythonOperator(
        task_id="load_traffic_counter_data_2",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_2",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

    load_traffic_counter_data_3 = PythonOperator(
        task_id="load_traffic_counter_data_3",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_3",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

    load_traffic_counter_data_4 = PythonOperator(
        task_id="load_traffic_counter_data_4",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_4",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

    load_traffic_counter_data_5 = PythonOperator(
        task_id="load_traffic_counter_data_5",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_5",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

    load_traffic_counter_data_6 = PythonOperator(
        task_id="load_traffic_counter_data_6",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_6",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table":
            "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter"
        },
        dag=dag,
    )

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

    load_gpt_data = PythonOperator(
        task_id="load_gpt_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": POSTGRESS_CONN_ID,
            "airflow_home": AIRFLOW_HOME,
            "location_data": LOCATION_DATA,
            "sublocation_data": SUBLOCATION_GPT_DATA,
            "file_name": "raw_gpt_data",
            "db_name": POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "gpt",
            "columns_table": "date, hour, place_id, name, lat, long, city, rating, rating_n, popularity_monday, popularity_tuesday, popularity_wednesday, popularity_thursday, popularity_friday, popularity_saturday, popularity_sunday, live, duration",
            "data_type": "gpt",
        },
        dag=dag,
    )

    bike_data >> load_bike_data
    charging_station_data >> load_charging_station_data
    traffic_counter_data_1 >> load_traffic_counter_data_1
    traffic_counter_data_2 >> load_traffic_counter_data_2
    traffic_counter_data_3 >> load_traffic_counter_data_3
    traffic_counter_data_4 >> load_traffic_counter_data_4
    traffic_counter_data_5 >> load_traffic_counter_data_5
    traffic_counter_data_6 >> load_traffic_counter_data_6
    parking_data >> load_parking_data
    gpt_data >> load_gpt_data

# ? 4.4. Finishing pipeline

finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> extract_transform_data

load_data >> finish_pipeline
