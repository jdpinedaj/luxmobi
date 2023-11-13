from datetime import datetime
import os
from typing import Optional

from .utils.logs import logger

from airflow.providers.postgres.hooks.postgres import PostgresHook


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
    date: Optional[str] = None,
    hour: Optional[str] = None,
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
    try:
        # If date and hour are not provided, use the current date and hour
        if not date or not hour:
            now = datetime.now()
            date = now.strftime("%Y%m%d")
            hour = now.strftime("%H")

        hook = PostgresHook(postgres_conn_id=postgres_conn_id)

        # Mapping of file numbers to road values
        road_mapping = {
            1: "B40",
            2: "A13",
            3: "A7",
            4: "A6",
            5: "A4",
            6: "A3",
        }

        for i in range(1, 7):  # Loop for files 1 to 6
            road = road_mapping[i] if data_type == "traffic_counter" else None
            file_suffix = (
                f"_raw_{data_type}_data_{i}.csv"
                if data_type == "traffic_counter"
                else f"_raw_{data_type}_data.csv"
            )
            file_full_name = (
                airflow_home
                + location_data
                + sublocation_data
                + date
                + "_"
                + hour
                + file_suffix
            )

            if not os.path.exists(file_full_name):
                logger.warning(f"The CSV file {file_full_name} does not exist.")
                continue

            with open(file_full_name, "r") as f:
                if data_type == "traffic_counter":
                    if (
                        road
                        and hook.get_records(
                            f"SELECT * FROM {db_name}.{schema_name}.{table_name} WHERE date = '{date}' and hour = '{hour}' and road = '{road}'"
                        )
                        == []
                    ):
                        next(f)  # Skip the header row
                        for line in f:
                            values = line.strip().split(",")

                            csv_date = values[0]
                            csv_hour = values[1]
                            id = values[2]
                            latitude = values[3]
                            longitude = values[4]
                            csv_road = values[5]
                            direction = values[6]
                            percentage = values[7]
                            speed = values[8]
                            vehicle_flow_rate = values[9]

                            sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{csv_date}', '{csv_hour}', '{id}', '{latitude}', '{longitude}', '{csv_road}', '{direction}', '{percentage}', '{speed}', '{vehicle_flow_rate}')"
                            hook.run(sql)
                    else:
                        print(f"Data for road {road} already exists in table")

                else:
                    if (
                        hook.get_records(
                            f"SELECT * FROM {db_name}.{schema_name}.{table_name} WHERE date = '{date}' and hour = '{hour}'"
                        )
                        == []
                    ):
                        next(f)  # Skip the header row
                        for line in f:
                            values = line.strip().split(",")
                            if data_type == "bike":
                                name = values[0]
                                date = values[1]
                                hour = values[2]
                                latitude = values[3]
                                longitude = values[4]
                                total_bike_stand = values[5]
                                bike_available = values[6]
                                bike_stands_available = values[7]

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{name}', '{date}', '{hour}', '{latitude}', '{longitude}', '{total_bike_stand}', '{bike_available}', '{bike_stands_available}')"

                            elif data_type == "stops_public_transport":
                                date = values[0]
                                hour = values[1]
                                name = values[2].replace(",", "").replace("'", "")
                                line = values[3]
                                catOut = values[4]
                                cls = values[5]
                                catOutS = values[6]
                                catOutL = values[7]
                                extid = values[8]
                                bus_stop = values[9].replace(",", "").replace("'", "")
                                latitude = values[10]
                                longitude = values[11]
                                weight = values[12]
                                dist = values[13]
                                products = values[14]

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{date}', '{hour}', '{name}', '{line}', '{catOut}', '{cls}', '{catOutS}', '{catOutL}', '{extid}', '{bus_stop}', '{latitude}', '{longitude}', '{weight}', '{dist}', '{products}')"

                            elif data_type == "departure_board":
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
                                operator = values[10].replace(",", "")
                                busName = values[11]
                                type = values[12]
                                stop = values[13].replace(",", "")
                                stopExtId = values[14]
                                direction = values[15].replace(",", "")
                                trainNumber = values[16]
                                trainCategory = values[17]

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{date}', '{hour}', '{name}', '{num}', '{line}', '{catOut}', '{catIn}', '{catCode}', '{cls}', '{operatorCode}', '{operator}', '{busName}', '{type}', '{stop}', '{stopExtId}', '{direction}', '{trainNumber}', '{trainCategory}')"

                            elif data_type == "charging_station":
                                date = values[0]
                                hour = values[1]
                                latitude = values[2]
                                longitude = values[3]
                                address = values[4]
                                occupied = values[5]
                                available = values[6]

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{date}', '{hour}', '{latitude}', '{longitude}', '{address}', '{occupied}', '{available}')"

                            elif data_type == "parking":
                                date = values[0]
                                hour = values[1]
                                name = values[2]
                                available = values[3]
                                total = values[4].replace("/", "")
                                occupancy = values[5]
                                trend = values[6]

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{date}', '{hour}', '{name}', '{available}', '{total}', '{occupancy}', '{trend}')"

                            elif data_type == "gpt":
                                date = values[0]
                                hour = values[1]
                                place_id = values[2]
                                name = values[3].replace("'", "")
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

                                sql = f"INSERT INTO {db_name}.{schema_name}.{table_name} ({columns_table}) VALUES ('{date}', '{hour}', '{place_id}', '{name}', '{latitude}', '{longitude}', '{city}', '{rating}', '{rating_n}', '{popularity_monday}', '{popularity_tuesday}', '{popularity_wednesday}', '{popularity_thursday}', '{popularity_friday}', '{popularity_saturday}', '{popularity_sunday}', '{live}', '{duration}')"

                            else:
                                raise ValueError("Invalid data type")

                            hook.run(sql)

                    else:
                        print("Data already exists in table")

        logger.info(f"Data loaded in table {table_name}")

    except Exception as e:
        logger.error(f"Error while loading data in table {table_name}: {e}")
        raise


def load_all_csv_files(
    postgres_conn_id: str,
    db_name: str,
    schema_name: str,
    table_name: str,
    columns_table: str,
    airflow_home: str,
    location_data: str,
    sublocation_data: str,
    data_type: Optional[str] = None,
) -> None:
    """
    This function loads all CSV files in a directory into a table in the database.
    Args:
        postgres_conn_id (str): postgres connection id
        db_name (str): name of the database
        schema_name (str): name of the schema
        table_name (str): name of the table
        columns_table (str): columns of the table
        airflow_home (str): path to airflow home
        location_data (str): path to location data
        sublocation_data (str): path to sublocation data
        data_type (str): type of data being inserted. Possible values: 'bike', 'stops_public_transport', 'departure_board', 'charging_station', 'traffic_counter', 'parking', 'gpt'
    Returns:
        None
    """
    try:
        directory_path = airflow_home + location_data + sublocation_data
        logger.info(f"Loading all CSV files in {directory_path}")

    except Exception as e:
        logger.error(f"Error while loading all CSV files in {directory_path}: {e}")
        raise

    try:
        # Iterate over all files in the directory
        for file_name in os.listdir(directory_path):
            # Check if the file is a CSV
            if file_name.endswith(".csv"):
                # Extracting date and hour from the file name
                parts = file_name.split("_")
                extracted_date = parts[0]
                extracted_hour = parts[1]

                # Removing the first date_hour part from the file name
                relevant_file_name = f"{'_'.join(parts[2:])}".replace(".csv", "")

                insert_data_to_table(
                    postgres_conn_id=postgres_conn_id,
                    db_name=db_name,
                    schema_name=schema_name,
                    table_name=table_name,
                    columns_table=columns_table,
                    airflow_home=airflow_home,
                    location_data=location_data,
                    sublocation_data=sublocation_data,
                    file_name=relevant_file_name,
                    data_type=data_type,
                    date=extracted_date,  # passing the extracted date
                    hour=extracted_hour,  # passing the extracted hour
                )
        logger.info(f"All CSV files in {directory_path} loaded")

    except Exception as e:
        logger.error(f"Error while loading all CSV files in {directory_path}: {e}")
        raise
