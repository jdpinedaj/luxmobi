from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import os


import scripts.config as config


# class MySqlToCsvOperator(BaseOperator):
    
#     @apply_defaults
#     def __init__(self,
#                  sql=None,
#                  target_table=None,
#                  identifier=None,
#                  mysql_conn_id=config.MYSQL_CONN_ID, 
#                  *args,
#                  **kwargs):
        
#         super().__init__(*args, **kwargs)
#         self.sql = sql
#         self.target_table = target_table
#         self.identifier = identifier
#         self.mysql_conn_id = mysql_conn_id

#     def execute(self, context):
        
#         start_date=context['data_interval_start'].strftime('%Y-%m-%d %H:%M:%S')
#         end_date=context['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')
        
#         self.sql = self.sql.format(start_date=start_date, end_date=end_date)
#         print("sql", self.sql)
        
#         source = MySqlHook(self.mysql_conn_id)
#         # Target is converting the data to csv and save it
#         target_df = pd.read_sql(self.sql, source.get_conn())
        
#         conn = source.get_conn()
#         cursor = conn.cursor()
        
#         cursor.execute(self.sql)
        
#         target_fields = [x[0] for x in cursor.description]
#         rows = cursor.fetchall()
        
#         # Saving the data to csv
#         target_df.to_csv('/opt/airflow/dags/data/csv_files/{}.csv'.format(self.target_table), index=False)
        
#         cursor.close()
#         conn.close()


class MySqlToCsvOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 sql=None,
                 mysql_conn_id=config.MYSQL_CONN_ID, 
                 csv_filepath=None,
                 *args,
                 **kwargs):
        
        super().__init__(*args, **kwargs)
        self.sql_file_path = self._origin_sql_path(sql) if sql.startswith("sql/") else None
        self.sql = sql if not sql.startswith("sql/") else None
        self.mysql_conn_id = mysql_conn_id
        self.csv_filepath = csv_filepath

    def execute(self, context):
        try:
            if self.sql_file_path:
                with open(self.sql_file_path, 'r') as file:
                    self.sql = file.read()

            data = self._get_data()
            self._write_csv(data)

            print("Data fetched from MySQL and written to CSV")
        except Exception as e:
            self.log.error("Failed to execute task: %s", str(e))
            raise

    def _origin_sql_path(self, sql):
        base_path = os.path.dirname(os.path.dirname(__file__)) 
        return os.path.join(base_path, sql)

    def _get_data(self):
        try:
            source = MySqlHook(self.mysql_conn_id)

            conn = source.get_conn()
            cursor = conn.cursor()

            cursor.execute(self.sql)

            self.target_fields = [x[0] for x in cursor.description]
            data = cursor.fetchall()

            cursor.close()
            conn.close()
            
            return data

        except Exception as e:
            self.log.error("Failed to fetch data from MySQL: %s", str(e))
            raise

    def _write_csv(self, data):
        try:
            os.makedirs(os.path.dirname(self.csv_filepath), exist_ok=True)
            
            df = pd.DataFrame(data, columns=self.target_fields)
            df.to_csv(self.csv_filepath, index=False)
        except Exception as e:
            self.log.error("Failed to write data to CSV: %s", str(e))
            raise


