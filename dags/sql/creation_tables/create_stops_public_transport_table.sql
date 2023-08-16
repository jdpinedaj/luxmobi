DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    line VARCHAR(255) NOT NULL,
    catOut VARCHAR(255) NOT NULL,
    cls INT NOT NULL,
    catOutS VARCHAR(255) NOT NULL,
    catOutL VARCHAR(255) NOT NULL,
    extID INT NOT NULL,
    bus_stop VARCHAR(255) NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    weight INT NOT NULL,
    dist INT NOT NULL,
    products INT NOT NULL
);

-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
