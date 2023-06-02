
-- Create the schema
DROP SCHEMA IF EXISTS {{ params.schema_name }} CASCADE;
CREATE SCHEMA IF NOT EXISTS {{ params.schema_name }};
-- ===================================


-- Revoke privileges for the regular_user role on the luxmobi database
REVOKE ALL PRIVILEGES ON DATABASE luxmobi FROM airflow;
REVOKE ALL PRIVILEGES ON DATABASE luxmobi FROM regular_user;


-- Remove the roles if they exist
-- ===================================

-- Remove the regular_user user if it exists
DROP ROLE IF EXISTS airflow;
DROP ROLE IF EXISTS regular_user;

-- Creating roles for the database
-- ===================================
CREATE ROLE regular_user LOGIN PASSWORD 'Mobilab123';
-- ===================================

-- Grant privileges to the roles
-- ===================================
-- Grant privileges to the regular_user user
GRANT CONNECT ON DATABASE {{ params.database_name }} TO regular_user;
GRANT USAGE ON SCHEMA {{ params.schema_name }} TO regular_user;
GRANT SELECT ON ALL TABLES IN SCHEMA {{ params.schema_name }} TO regular_user;
