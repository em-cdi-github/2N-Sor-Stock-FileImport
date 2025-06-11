USE ROLE sysadmin;

-- Creating {{ database_name.upper() }}.{{ schema_name.upper() }} schema
CREATE SCHEMA IF NOT EXISTS {{ database_name.upper() }}.{{ schema_name.upper() }}
;