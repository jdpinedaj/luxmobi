from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import os

import scripts.config as config


class MySqlToPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(
        self,
        sql=None,
        target_table=None,
        identifier=None,
        mysql_conn_id=config.MYSQL_CONN_ID,
        postgres_conn_id=config.POSTGRESS_CONN_ID,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.sql_file_path = (
            self._origin_sql_path(sql) if sql.startswith("sql/") else None
        )
        self.sql = sql if not sql.startswith("sql/") else None
        self.target_table = target_table
        self.identifier = identifier
        self.mysql_conn_id = mysql_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.target_hook = PostgresHook(self.postgres_conn_id)

    def execute(self, context):
        if self.sql_file_path:
            with open(self.sql_file_path, "r") as file:
                self.sql = file.read()

        self.transfer_data_in_batches()

        print("Data transferred from MySQL to Postgres")

    def _origin_sql_path(self, sql):
        base_path = os.path.dirname(os.path.dirname(__file__))
        return os.path.join(base_path, sql)

    def transfer_data_in_batches(self, batch_size=100):
        source = MySqlHook(self.mysql_conn_id)

        conn = source.get_conn()
        cursor = conn.cursor()

        cursor.execute(self.sql)
        self.target_fields = [x[0] for x in cursor.description]

        batch = cursor.fetchmany(batch_size)

        while batch:
            self._write_data(batch)
            batch = cursor.fetchmany(batch_size)

        cursor.close()
        conn.close()

    def _write_data(self, data):
        self.target_hook.insert_rows(
            self.target_table,
            data,
            target_fields=self.target_fields,
            replace_index=self.identifier,
            replace=True,
        )
