#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python import PythonOperator

# Mysql
# from airflow.providers.mysql.hooks.mysql import MySqlHook
# from airflow.providers.mysql.operators.mysql import MySqlOperator
# from custom_operators.MySqlToPostgresOperator import MySqlToPostgresOperator
from custom_operators.MySqlToCsvOperator import MySqlToCsvOperator
from custom_operators.CsvToPostgresOperator import CsvToPostgresOperator
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
    dag_id="workflow_integration_db",
    description="workflow_integration_db",
    start_date=datetime(2023, 9, 24, 7, 0, 0),
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

# # ? 4.2. Reading database mysql
# read_mysql = MySqlOperator(
#     task_id="read_mysql",
#     mysql_conn_id=config.MYSQL_CONN_ID,
#     sql="/sql/previous_tables/gpt_activity_table.sql",
#     params={"table_name": "luxmob.gpt_activity"},
#     dag=dag,
# )

# # ? 4.2. MySQL to CSV
fetch_data = MySqlToCsvOperator(
    task_id="fetch_data",
    sql="sql/previous_tables/gpt_activity_table.sql",
    csv_filepath="data/csv_files/gpt_activity.csv",
    dag=dag,
)

load_data = CsvToPostgresOperator(
    task_id="load_data",
    csv_filepath="data/csv_files/gpt_activity.csv",
    target_table="luxmobi.raw.gpt",
    postgres_conn_id="postgres_default",
)

# TODO: Pass queries to files
# ? 4.3. Transfer data from MySQL to Postgres
# transfer_data = MySqlToPostgresOperator(
#     task_id="transfer_data",
#     sql="sql/previous_tables/gpt_activity_table.sql",
#     target_table="luxmobi.raw.gpt",
#     identifier=["date", "hour", "place_id"],
#     dag=dag,
# )


# ? 4.5. Finishing pipeline
finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> fetch_data >> load_data >> finish_pipeline
