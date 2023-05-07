DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    name VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    hour INT NOT NULL,
    lat FLOAT NOT NULL,
    long FLOAT NOT NULL,
    total_bike_stand INT NOT NULL,
    bike_available INT NOT NULL,
    bike_stands_available INT NOT NULL
);