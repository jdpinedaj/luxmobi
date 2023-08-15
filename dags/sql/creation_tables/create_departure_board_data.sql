DROP TABLE IF EXISTS {{ params.table_name }};
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    date DATE NOT NULL,
    hour INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    num INT NOT NULL,
    line VARCHAR(255) NOT NULL,
    catOut VARCHAR(255) NOT NULL,
    catIn VARCHAR(255) NOT NULL,
    catCode INT NOT NULL,
    cls INT NOT NULL,
    operatorCode VARCHAR(255) NOT NULL,
    operator VARCHAR(255) NOT NULL,
    busName VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    stop VARCHAR(255) NOT NULL,
    stopExtId INT NOT NULL,
    direction VARCHAR(255) NOT NULL,
    trainNumber INT NOT NULL,
    trainCategory VARCHAR(255) NOT NULL
);

-- Granting access to the table to regular_user ROLE
GRANT SELECT ON TABLE {{ params.table_name }} TO regular_user;
