-- Create databases for analytics infrastructure

-- Create Hive metastore database
CREATE DATABASE hive_metastore;
GRANT ALL PRIVILEGES ON DATABASE hive_metastore TO analytics;

-- Create Druid metadata database (already exists as analytics_metadata)
-- GRANT ALL PRIVILEGES ON DATABASE analytics_metadata TO analytics;

-- Create Superset database
CREATE DATABASE superset;
GRANT ALL PRIVILEGES ON DATABASE superset TO analytics;

-- Switch to analytics_metadata database
\c analytics_metadata;

-- Create schema for analytics metadata
CREATE SCHEMA IF NOT EXISTS druid;
GRANT ALL ON SCHEMA druid TO analytics;

-- Switch to hive_metastore database
\c hive_metastore;

-- Create schema for hive
CREATE SCHEMA IF NOT EXISTS hive;
GRANT ALL ON SCHEMA hive TO analytics; 