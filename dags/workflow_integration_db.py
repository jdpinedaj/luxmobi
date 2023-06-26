#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from scripts.transfer_data import transfer_data_mysql_to_postgres
from custom_operators.MySqlToPostgreOperator import MySqlToPostgreOperator

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
    dag_id="workflow_integration_db",
    description="workflow_integration_db",
    start_date=datetime(2023, 5, 24, 7, 0, 0),
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

# ? 4.2. Transfer data from MySQL to Postgres
transfer_data = MySqlToPostgreOperator(
    task_id="transfer_data",
    sql="sql/transfer_data.sql",
    target_table="luxmobi.raw.gpt",
    identifier="id",
    dag=dag,
)


# ? 4.5. Finishing pipeline
finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline  >> transfer_data >> finish_pipeline

    

