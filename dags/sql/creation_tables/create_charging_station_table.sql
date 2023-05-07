DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    lat FLOAT NOT NULL,
    long FLOAT NOT NULL,
    address VARCHAR(255) NOT NULL,
    occupied INT NOT NULL,
    available INT NOT NULL
);