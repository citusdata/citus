-- Simple PostgreSQL 18 test for "?column?" issue
CREATE SCHEMA test_simple;
SET search_path TO test_simple;

CREATE TABLE simple_table (
    id INTEGER,
    value TEXT
);

SELECT create_distributed_table('simple_table', 'id');

INSERT INTO simple_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Test a simple subquery that might trigger the "?column?" issue
SELECT * FROM (
    SELECT id FROM simple_table 
) AS sub;

-- Test more complex nested subquery
SELECT * FROM (
    SELECT id FROM (
        SELECT id, value FROM simple_table
    ) inner_sub
) outer_sub;

DROP SCHEMA test_simple CASCADE;
