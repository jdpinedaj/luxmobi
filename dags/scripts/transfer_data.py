
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook

import scripts.config as config


def transfer_data_mysql_to_postgres(ti):
    src = MySqlHook(mysql_conn_id=config.MYSQL_CONN_ID)
    dest = PostgresHook(postgres_conn_id=config.POSTGRESS_CONN_ID)

    src_conn = src.get_conn()
    cursor = src_conn.cursor()
    with open('../sql/previous_tables.sql', 'r') as f:
        sql = f.read().format(
            table_name='luxmob.gpt_activity')
    cursor.execute(sql)
    result = cursor.fetchall()

    dest.insert_rows(table='luxmobi.raw.gpt', rows=result)