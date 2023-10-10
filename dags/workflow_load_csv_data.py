#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from scripts.load_data import load_all_csv_files
import scripts.config as config


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
    start_date=datetime(2023, 9, 20, 8, 0, 0),
    schedule_interval=config.SCHEDULE_INTERVAL_ONCE,  # SCHEDULE_INTERVAL_HOURLY or SCHEDULE_INTERVAL_ONCE
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


# ? 4.2. Loading data

with TaskGroup(
    "load_data",
    dag=dag,
) as load_data:
    load_bike_data = PythonOperator(
        task_id="load_bike_data",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_BIKE_DATA,
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
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_STOPS_PUBLIC_TRANSPORT_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "stops_public_transport",
            "columns_table": "date, hour, name, line, catout, cls, catouts, catoutl, extid, bus_stop, latitude, longitude, weight, dist, products",
            "data_type": "stops_public_transport",
        },
        dag=dag,
    )

    load_departure_board_data = PythonOperator(
        task_id="load_departure_board_data",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_DEPARTURE_BOARD_DATA,
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
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_CHARGING_STATION_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "charging_station",
            "columns_table": "date, hour, latitude, longitude, address, occupied, available",
            "data_type": "charging_station",
        },
        dag=dag,
    )

    load_traffic_counter_data_1 = PythonOperator(
        task_id="load_traffic_counter_data_1",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_traffic_counter_data_2 = PythonOperator(
        task_id="load_traffic_counter_data_2",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_traffic_counter_data_3 = PythonOperator(
        task_id="load_traffic_counter_data_3",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_traffic_counter_data_4 = PythonOperator(
        task_id="load_traffic_counter_data_4",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_traffic_counter_data_5 = PythonOperator(
        task_id="load_traffic_counter_data_5",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_traffic_counter_data_6 = PythonOperator(
        task_id="load_traffic_counter_data_6",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_TRAFFIC_COUNTER_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "traffic_counter",
            "columns_table": "date, hour, id, latitude, longitude, road, direction, percentage, speed, vehicle_flow_rate",
            "data_type": "traffic_counter",
        },
        dag=dag,
    )

    load_parking_data = PythonOperator(
        task_id="load_parking_data",
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_PARKING_DATA,
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
        python_callable=load_all_csv_files,
        op_kwargs={
            "postgres_conn_id": config.POSTGRESS_CONN_ID,
            "airflow_home": config.AIRFLOW_HOME,
            "location_data": config.LOCATION_DATA,
            "sublocation_data": config.SUBLOCATION_GPT_DATA,
            "db_name": config.POSTGRES_DBNAME,
            "schema_name": "raw",
            "table_name": "gpt",
            "columns_table": "date, hour, place_id, name, latitude, longitude, city, rating, rating_n, popularity_monday, popularity_tuesday, popularity_wednesday, popularity_thursday, popularity_friday, popularity_saturday, popularity_sunday, live, duration",
            "data_type": "gpt",
        },
        dag=dag,
    )


# ? 4.3. Finishing pipeline

finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> load_data >> finish_pipeline
