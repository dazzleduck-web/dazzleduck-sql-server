-- OllyLake startup script for DazzleDuck
-- Sets up the ollylake catalog with metric and log tables

INSTALL arrow FROM community;
LOAD arrow;
INSTALL ducklake;
LOAD ducklake;
-- Retry a commit that loses the race for the next snapshot id up to 30 times (default 10): with
-- several writers on one lake, DuckDB 2.0 commits collide more often than 1.5's did.
SET ducklake_max_retry_count = 30;

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
    sequence_number BIGINT,
    timestamp TIMESTAMP,
    level VARCHAR,
    logger VARCHAR,
    thread VARCHAR,
    message VARCHAR,
    mdc MAP(VARCHAR, VARCHAR),
    throwable VARCHAR,
    marker VARCHAR[],
    key_value_pairs MAP(VARCHAR, VARCHAR),
    caller_class VARCHAR,
    caller_method VARCHAR,
    caller_file VARCHAR,
    caller_line INTEGER,
    date DATE
);
ALTER TABLE ollylake.main.log SET PARTITIONED BY (date);
