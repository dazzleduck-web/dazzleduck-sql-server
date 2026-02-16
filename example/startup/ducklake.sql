-- Do not change the content of this file. Many tests are based on the content
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:/data/ducklake_demo/metadata.ducklake' AS demo_db (DATA_PATH '/data/ducklake_demo');

-- Simple demo table for basic split mode tests
CREATE TABLE demo_db.main.demo (key STRING, value STRING, partition INT);
ALTER TABLE demo_db.main.demo SET PARTITIONED BY (partition);
INSERT INTO demo_db.main.demo VALUES
    ('k00', 'v00', 0),
    ('k01', 'v01', 0),
    ('k51', 'v51', 1),
    ('k61', 'v61', 1);

-- Comprehensive table with all numeric types for aggregation testing
CREATE TABLE demo_db.main.all_types (
    id INTEGER,
    tiny_col TINYINT,
    small_col SMALLINT,
    int_col INTEGER,
    big_col BIGINT,
    float_col FLOAT,
    double_col DOUBLE,
    decimal_small DECIMAL(4,2),
    decimal_medium DECIMAL(9,3),
    decimal_large DECIMAL(18,4),
    decimal_huge DECIMAL(38,10),
    partition INT
);
ALTER TABLE demo_db.main.all_types SET PARTITIONED BY (partition);

-- Insert test data with variety of values including NULLs
INSERT INTO demo_db.main.all_types VALUES
    -- Partition 0: regular values
    (1, 10, 100, 1000, 10000, 1.5, 10.5, 12.34, 123.456, 1234.5678, 12345678.9012345678, 0),
    (2, 20, 200, 2000, 20000, 2.5, 20.5, 23.45, 234.567, 2345.6789, 23456789.0123456789, 0),
    (3, 30, 300, 3000, 30000, 3.5, 30.5, 34.56, 345.678, 3456.7890, 34567890.1234567890, 0),
    -- Partition 0: with NULLs
    (4, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0),
    (5, 50, 500, 5000, 50000, 5.5, 50.5, 56.78, 567.890, 5678.9012, 56789012.3456789012, 0),
    -- Partition 1: regular values
    (6, 60, 600, 6000, 60000, 6.5, 60.5, 67.89, 678.901, 6789.0123, 67890123.4567890123, 1),
    (7, 70, 700, 7000, 70000, 7.5, 70.5, 78.90, 789.012, 7890.1234, 78901234.5678901234, 1),
    -- Partition 1: negative values
    (8, -80, -800, -8000, -80000, -8.5, -80.5, -89.01, -890.123, -8901.2345, -89012345.6789012345, 1),
    -- Partition 1: with NULLs
    (9, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 1),
    (10, 100, 1000, 10000, 100000, 10.5, 100.5, 99.99, 999.999, 9999.9999, 99999999.9999999999, 1);
