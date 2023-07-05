from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import os
import csv

import scripts.config as config


class CsvToPostgresOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 csv_filepath=None,
                 target_table=None,
                 postgres_conn_id=config.POSTGRESS_CONN_ID,
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        self.csv_filepath = csv_filepath
        self.target_table = target_table
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        self._write_data()

        print("Data loaded from CSV to Postgres")

    def _write_data(self):
        postgres = PostgresHook(self.postgres_conn_id)

        with open(self.csv_filepath, 'r') as f:
            reader = csv.reader(f)
            fields = next(reader) # get the headers
            data = list(reader)   # get the rest of the data

        # Now, create SQL for each row and execute it
        for row in data:
            # Create a dict to easily map field names to values
            row_dict = dict(zip(fields, row))

            # Construct an SQL query
            sql = """
                INSERT INTO {table} ({columns})
                VALUES ({values})
                ON CONFLICT (date, hour, place_id) DO NOTHING;
            """.format(
                table=self.target_table,
                columns=", ".join(fields),
                values=", ".join(f"'{row_dict[field]}'" for field in fields)  # assuming all fields are strings, adjust as needed
            )

            # Execute the SQL
            postgres.run(sql)