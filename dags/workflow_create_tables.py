#######################
##! 1. Importing modules
#######################

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
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
    dag_id="create_tables",
    description="create_tables",
    start_date=datetime(2023, 5, 4),
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

# ? 4.2. Creating empty tables

with TaskGroup(
        "create_tables",
        dag=dag,
) as create_tables:
    create_public_schema = PostgresOperator(
        task_id="create_public_schema",
        postgres_conn_id="postgres_default",
        sql="sql/create_schema.sql",
        params={"database_name": "luxmobi",
        "schema_name": "raw"},
        dag=dag,
    )

    create_bike_table = PostgresOperator(
        task_id="create_bike_table",
        postgres_conn_id="postgres_default",
        sql="sql/creation_tables/create_bike_table.sql",
        params={"table_name": "luxmobi.raw.bike"},
        dag=dag,
    )

    create_charging_station_table = PostgresOperator(
        task_id="create_charging_station_table",
        postgres_conn_id="postgres_default",
        sql="sql/creation_tables/create_charging_station_table.sql",
        params={"table_name": "luxmobi.raw.charging_station"},
        dag=dag,
    )

    create_parking_table = PostgresOperator(
        task_id="create_parking_table",
        postgres_conn_id="postgres_default",
        sql="sql/creation_tables/create_parking_table.sql",
        params={"table_name": "luxmobi.raw.parking"},
        dag=dag,
    )

    create_traffic_counter_table = PostgresOperator(
        task_id="create_traffic_counter_table",
        postgres_conn_id="postgres_default",
        sql="sql/creation_tables/create_traffic_counter_table.sql",
        params={"table_name": "luxmobi.raw.traffic_counter"},
        dag=dag,
    )

    create_gpt_table = PostgresOperator(
        task_id="create_gpt_table",
        postgres_conn_id="postgres_default",
        sql="sql/creation_tables/create_gpt_table.sql",
        params={"table_name": "luxmobi.raw.gpt"},
        dag=dag,
    )

    create_public_schema >> [
        create_bike_table,
        create_charging_station_table,
        create_parking_table,
        create_traffic_counter_table,
        create_gpt_table,
    ]

# ? 4.5. Finishing pipeline

finish_pipeline = DummyOperator(
    task_id="finish_pipeline",
    dag=dag,
)

#######################
##! 5. Setting up dependencies
#######################

start_pipeline >> create_tables

create_tables >> finish_pipeline
