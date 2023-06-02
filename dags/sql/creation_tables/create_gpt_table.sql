DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    city VARCHAR(255) NOT NULL,
    id VARCHAR(255) NOT NULL,
    rating VARCHAR(255) NULL,
    rating_n VARCHAR(255) NULL,
    popularity VARCHAR(255) NULL,
    live VARCHAR(255) NULL,
    duration VARCHAR(255) NULL
);


-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
