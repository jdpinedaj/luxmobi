from typing import Optional

# from dags.scripts.utils.logs import logger

from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime


def insert_data_to_table(
    postgres_conn_id: str,
    db_name: str,
    schema_name: str,
    table_name: str,
    columns_table: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    file_name: str,
    data_type: Optional[str] = None,
) -> None:
    """
    This function inserts the bike or charging station data into a table in the database.
    Args:
        postgres_conn_id (str): postgres connection id
        db_name (str): name of the database
        schema_name (str): name of the schema
        table_name (str): name of the table
        columns_table (str): columns of the table
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        file_name (str): name of the file
        data_type (str): type of data being inserted. Possible values: 'bike', 'charging_station', 'traffic_counter' # TODO!!!!
    Returns:
        None
    """
    now = datetime.now()
    date = now.strftime("%Y%m%d")
    hour = now.strftime("%H")

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    with open(
            airflow_home + location_data + sublocation_data + date + "_" +
            hour + "_" + file_name + ".csv",
            'r',
    ) as f:
        if hook.get_records(
                f"SELECT * FROM {db_name}.{schema_name}.{table_name} WHERE date = '{date}' and hour = '{hour}'"
        ) == []:
            next(f)  # Skip the header row
            for line in f:
                values = line.strip().split(',')
                if data_type == 'bike':
                    name = values[0]
                    date = values[1]
                    hour = values[2]
                    lat = values[3]
                    long = values[4]
                    total_bike_stand = values[5]
                    bike_available = values[6]
                    bike_stands_available = values[7]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{name}\', \'{date}\', \'{hour}\', \'{lat}\', \'{long}\', \'{total_bike_stand}\', \'{bike_available}\', \'{bike_stands_available}\')'

                elif data_type == 'charging_station':
                    date = values[0]
                    hour = values[1]
                    lat = values[2]
                    long = values[3]
                    address = values[4]
                    occupied = values[5]
                    available = values[6]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{lat}\', \'{long}\', \'{address}\', \'{occupied}\', \'{available}\')'

                elif data_type == 'traffic_counter':
                    date = values[0]
                    hour = values[1]
                    id = values[2]
                    lat = values[3]
                    long = values[4]
                    road = values[5]
                    direction = values[6]
                    percentage = values[7]
                    speed = values[8]
                    vehicle_flow_rate = values[9]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{id}\', \'{lat}\', \'{long}\', \'{road}\', \'{direction}\', \'{percentage}\', \'{speed}\', \'{vehicle_flow_rate}\')'

                else:
                    raise ValueError("Invalid data type")

                hook.run(sql)

        else:
            print("Data already exists in table")
