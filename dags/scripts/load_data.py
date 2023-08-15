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
        data_type (str): type of data being inserted. Possible values: 'bike', 'stops_public_transport', 'departure_board', 'charging_station', 'traffic_counter', 'parking', 'gpt'
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
                    latitude = values[3]
                    longitude = values[4]
                    total_bike_stand = values[5]
                    bike_available = values[6]
                    bike_stands_available = values[7]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{name}\', \'{date}\', \'{hour}\', \'{latitude}\', \'{longitude}\', \'{total_bike_stand}\', \'{bike_available}\', \'{bike_stands_available}\')'

                elif data_type == 'stops_public_transport':
                    date = values[0]
                    hour = values[1]
                    name = values[2]
                    line = values[3]
                    catOut = values[4]
                    cls = values[5]
                    catOutS = values[6]
                    catOutL = values[7]
                    extid = values[7]
                    bus_stop = values[9]
                    latitude = values[10]
                    longitude = values[11]
                    weight = values[12]
                    dist = values[13]
                    products = values[14]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{name}\', \'{line}\', \'{catOut}\', \'{cls}\', \'{catOutS}\', \'{catOutL}\', \'{extid}\', \'{bus_stop}\', \'{latitude}\', \'{longitude}\', \'{weight}\', \'{dist}\', \'{products}\')'

                elif data_type == 'departure_board':
                    date = values[0]
                    hour = values[1]
                    name = values[2]
                    num = values[3]
                    line = values[4]
                    catOut = values[5]
                    catIn = values[6]
                    catCode = values[7]
                    cls = values[8]
                    operatorCode = values[9]
                    operator = values[10]
                    busName = values[11]
                    type = values[12]
                    stop = values[13]
                    stopExtId = values[14]
                    direction = values[15]
                    trainNumber = values[16]
                    trainCategory = values[17]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{name}\', \'{num}\', \'{line}\', \'{catOut}\', \'{catIn}\', \'{catCode}\', \'{cls}\', \'{operatorCode}\', \'{operator}\', \'{busName}\', \'{type}\', \'{stop}\', \'{stopExtId}\', \'{direction}\', \'{trainNumber}\', \'{trainCategory}\')'

                elif data_type == 'charging_station':
                    date = values[0]
                    hour = values[1]
                    latitude = values[2]
                    longitude = values[3]
                    address = values[4]
                    occupied = values[5]
                    available = values[6]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{latitude}\', \'{longitude}\', \'{address}\', \'{occupied}\', \'{available}\')'

                elif data_type == 'traffic_counter':
                    date = values[0]
                    hour = values[1]
                    id = values[2]
                    latitude = values[3]
                    longitude = values[4]
                    road = values[5]
                    direction = values[6]
                    percentage = values[7]
                    speed = values[8]
                    vehicle_flow_rate = values[9]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{id}\', \'{latitude}\', \'{longitude}\', \'{road}\', \'{direction}\', \'{percentage}\', \'{speed}\', \'{vehicle_flow_rate}\')'

                elif data_type == 'parking':
                    date = values[0]
                    hour = values[1]
                    name = values[2]
                    available = values[3]
                    total = values[4]
                    occupancy = values[5]
                    trend = values[6]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{name}\', \'{available}\', \'{total}\', \'{occupancy}\', \'{trend}\')'

                elif data_type == 'gpt':
                    date = values[0]
                    hour = values[1]
                    place_id = values[2]
                    name = values[3]
                    latitude = values[4]
                    longitude = values[5]
                    city = values[6]
                    rating = values[7]
                    rating_n = values[8]
                    popularity_monday = values[9]
                    popularity_tuesday = values[10]
                    popularity_wednesday = values[11]
                    popularity_thursday = values[12]
                    popularity_friday = values[13]
                    popularity_saturday = values[14]
                    popularity_sunday = values[15]
                    live = values[16]
                    duration = values[17]

                    sql = f'INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES (\'{date}\', \'{hour}\', \'{place_id}\', \'{name}\', \'{latitude}\', \'{longitude}\', \'{city}\', \'{rating}\', \'{rating_n}\', \'{popularity_monday}\', \'{popularity_tuesday}\', \'{popularity_wednesday}\', \'{popularity_thursday}\', \'{popularity_friday}\', \'{popularity_saturday}\', \'{popularity_sunday}\', \'{live}\', \'{duration}\')'
                    
                else:
                    raise ValueError("Invalid data type")

                hook.run(sql)

        else:
            print("Data already exists in table")
