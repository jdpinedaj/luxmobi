DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    id VARCHAR(255) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    road VARCHAR(255) NOT NULL,
    direction VARCHAR(255) NOT NULL,
    percentage FLOAT NOT NULL,
    speed FLOAT NOT NULL,
    vehicle_flow_rate INT NOT NULL
);

-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
