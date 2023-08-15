#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from scripts.extract_transform_data import (
    # test_webdriver,
    extraction_bike_data,
    extraction_stops_public_transport_data,
    extraction_departure_board_data,
    extraction_charging_station_data,
    extraction_traffic_counter_data,
    extraction_parking_data,
    extraction_gpt_data,   
)
from scripts.load_data import insert_data_to_table
import scripts.config as config
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

#######################
##! 3. Instantiate a DAG
#######################

dag = DAG(
    dag_id="workflow_ETL",
    description="workflow_ETL",
    start_date=datetime(2023, 8, 14, 7, 0, 0),
    #TODO: Once it is testes, change to config.SCHEDULE_INTERVAL_HOURLY
    schedule_interval=config.SCHEDULE_INTERVAL_ONCE,
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
#         "chromedriver_path": config.CHROMEDRIVER_PATH,
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
            "url": config.URL_BIKE,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_BIKE_DATA,
            "file_name": "raw_bike_data",
            "columns": [
                "name",
                "date",
                "hour",
                "latitude",
                "longitude",
                "bike_stands",
                "available_bikes",
                "available_bike_stands",
            ],
        },
        dag=dag,
    )

    stops_public_transport_data = PythonOperator(
        task_id="stops_public_transport_data",
        python_callable=extraction_stops_public_transport_data,
        op_kwargs={
            "url": config.URL_STOPS_PUBLIC_TRANSPORT,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_STOPS_PUBLIC_TRANSPORT_DATA,
            "file_name": "raw_stops_public_transport_data",
            "columns": [
                "date",
                "hour",
                "extid"
                "name",
                "line",
                "catOut",
                "cls",
                "catOutS",
                "catOutL",
                "bus_stop",
                "latitude",
                "longitude",
                "weight",
                "dist",
                "products",
            ],
        },
        dag=dag,
    )

    departure_board_data = PythonOperator(
        task_id="departure_board_data",
        python_callable=extraction_departure_board_data,
        op_kwargs={
            "url": config.URL_DEPARTURE_BOARD,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_DEPARTURE_BOARD_DATA,
            "file_name": "raw_departure_board_data",
            "columns": [
                "date",
                "hour",
                "name",
                "num",
                "line",
                "catOut",
                "catIn",
                "catCode",
                "cls",
                "operatorCode",
                "operator",
                "busName",
                "type",
                "stop",
                "stopExtId",
                "direction",
                "trainNumber",
                "trainCategory",
            ],
        },
        dag=dag,
    )

    charging_station_data = PythonOperator(
        task_id="charging_station_data",
        python_callable=extraction_charging_station_data,
        op_kwargs={
            "url": config.URL_CHARGING_STATION,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_CHARGING_STATION_DATA,
            "file_name": "raw_charging_station_data",
            "columns" : [
        "date",
        "hour",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_1,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_1",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_2,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_2",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_3,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_3",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_4,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_4",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_5,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_5",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_TRAFFIC_COUNTER_DATA_6,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_6",
            "columns": [
        "date",
        "hour",
        "id",
        "latitude",
        "longitude",
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
            "url": config.URL_PARKING_DATA,
            "chromedriver_path": config.CHROMEDRIVER_PATH,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_PARKING_DATA,
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
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_cache": config.SUBLOCATION_GPT_DATA_CACHE,
            "sublocation_data": config.SUBLOCATION_GPT_DATA,
            "file_name": "raw_gpt_data",
            "columns": [
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_BIKE_DATA,
            "file_name": "raw_bike_data",
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "bike",
            "columns_table": "name, date, hour, latitude, longitude, total_bike_stand, bike_available, bike_stands_available",
            "data_type": "bike",
        },
        dag=dag,
    )

    load_stops_public_transport_data = PythonOperator(
        task_id="load_stops_public_transport_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_STOPS_PUBLIC_TRANSPORT_DATA,
            "file_name": "raw_stops_public_transport_data",
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "stops_public_transport",
            "columns_table": "date, hour, extid, name, line, catout, cls, catouts, catoutl, bus_stop, latitude, longitude, weight, dist, products",
            "data_type": "stops_public_transport",
        },
        dag=dag,
    )

    load_departure_board_data = PythonOperator(
        task_id="load_departure_board_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "sublocation_data": config.SUBLOCATION_DEPARTURE_BOARD_DATA,
            "file_name": "raw_departure_board_data",
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "departure_board",
            "columns_table": "date, hour, name, num, line, catout, catin, catcode, cls, operatorcode, operator, busname, type, stop, stopextid, direction, trainnumber, traincategory",
            "data_type": "departure_board",
        },
        dag=dag,
    )

    load_charging_station_data = PythonOperator(
        task_id="load_charging_station_data",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_CHARGING_STATION_DATA,
            "file_name": "raw_charging_station_data",
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "charging_station",
            "columns_table":
            "date, hour, latitude, longitude, address, occupied, available",
            "data_type": "charging_station"
        },
        dag=dag,
    )

    load_traffic_counter_data_1 = PythonOperator(
        task_id="load_traffic_counter_data_1",
        python_callable=insert_data_to_table,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_1",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_2",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_3",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_4",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_5",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "file_name": "raw_traffic_counter_data_6",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_PARKING_DATA,
            "file_name": "raw_parking_data",
            "db_name": config.POSTGRES_DBNAME,
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
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_GPT_DATA,
            "file_name": "raw_gpt_data",
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "gpt",
            "columns_table": "date, hour, place_id, name, latitude, longitude, city, rating, rating_n, popularity_monday, popularity_tuesday, popularity_wednesday, popularity_thursday, popularity_friday, popularity_saturday, popularity_sunday, live, duration",
            "data_type": "gpt",
        },
        dag=dag,
    )

    bike_data >> load_bike_data
    stops_public_transport_data >> load_stops_public_transport_data
    departure_board_data >> load_departure_board_data
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
