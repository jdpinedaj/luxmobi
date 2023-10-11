from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import config


default_args = {
    "owner": "juan.pineda",
    "depends_on_past": False,
    "email": ["juan.pineda@uni.lu"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
}

with DAG(
    dag_id="workflow_test_dag",
    description="workflow_test_dag",
    start_date=datetime(2023, 9, 20),
    schedule_interval=config.SCHEDULE_INTERVAL_ONCE,
    concurrency=5,
    max_active_runs=1,
    default_args=default_args,
) as dag1:
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command="echo 1",
    )

    run_this_last = BashOperator(
        task_id="run_this_last",
        bash_command="echo 2",
    )


run_this >> run_this_last
