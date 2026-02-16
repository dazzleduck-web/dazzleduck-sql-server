-- OllyLake startup script for DazzleDuck
-- Sets up the ollylake catalog with metric and log tables

INSTALL arrow FROM community;
LOAD arrow;
INSTALL ducklake;
LOAD ducklake;

-- Attach ollylake catalog
ATTACH 'ducklake:/workspace/warehouse/metadata/ollylake/metadata.ducklake' AS ollylake (DATA_PATH '/workspace/warehouse/data/ollylake');

-- Create metric table for Micrometer metrics
CREATE TABLE IF NOT EXISTS ollylake.main.metric (
    timestamp TIMESTAMP,
    name VARCHAR,
    type VARCHAR,
    tags MAP(VARCHAR, VARCHAR),
    value DOUBLE,
    min DOUBLE,
    max DOUBLE,
    mean DOUBLE,
    date DATE
);
ALTER TABLE ollylake.main.metric SET PARTITIONED BY (date);

-- Create log table for Logback logs
CREATE TABLE IF NOT EXISTS ollylake.main.log (
    s_no BIGINT,
    timestamp TIMESTAMP,
    level VARCHAR,
    logger VARCHAR,
    thread VARCHAR,
    message VARCHAR,
    mdc MAP(VARCHAR, VARCHAR),
    date DATE
);
ALTER TABLE ollylake.main.log SET PARTITIONED BY (date);
