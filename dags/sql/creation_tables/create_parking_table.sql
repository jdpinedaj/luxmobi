-- DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    available INT NOT NULL,
    total INT NOT NULL,
    occupancy FLOAT NOT NULL,
    trend VARCHAR(255) NOT NULL
);

-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
