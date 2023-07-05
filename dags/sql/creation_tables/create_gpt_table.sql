DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    place_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    latitude VARCHAR(255) NOT NULL,
    longitude VARCHAR(255) NOT NULL,
    city VARCHAR(255) NOT NULL,
    rating VARCHAR(255) NULL,
    rating_n VARCHAR(255) NULL,
    popularity_monday VARCHAR(255) NULL,
    popularity_tuesday VARCHAR(255) NULL,
    popularity_wednesday VARCHAR(255) NULL,
    popularity_thursday VARCHAR(255) NULL,
    popularity_friday VARCHAR(255) NULL,
    popularity_saturday VARCHAR(255) NULL,
    popularity_sunday VARCHAR(255) NULL,
    live VARCHAR(255) NULL,
    duration VARCHAR(255) NULL,
    CONSTRAINT unique_datetime_place_id UNIQUE (date, hour, place_id)
);


-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
