-- DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    name VARCHAR(255) NOT NULL,
    date DATE NOT NULL,
    hour INT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    total_bike_stand INT NOT NULL,
    bike_available INT NOT NULL,
    bike_stands_available INT NOT NULL
);

-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
