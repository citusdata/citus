--
-- Testing ALTER TABLE on cstore_fdw tables.
--

CREATE FOREIGN TABLE test_alter_table (a int, b int, c int) SERVER cstore_server;

WITH sample_data AS (VALUES
    (1, 2, 3),
    (4, 5, 6),
    (7, 8, 9)
)
INSERT INTO test_alter_table SELECT * FROM sample_data;

-- drop a column
ALTER FOREIGN TABLE test_alter_table DROP COLUMN a;

-- test analyze
ANALYZE test_alter_table;

-- verify select queries run as expected
SELECT * FROM test_alter_table;
SELECT a FROM test_alter_table;
SELECT b FROM test_alter_table;

-- verify insert runs as expected
INSERT INTO test_alter_table (SELECT 3, 5, 8);
INSERT INTO test_alter_table (SELECT 5, 8);


-- add a column with no defaults
ALTER FOREIGN TABLE test_alter_table ADD COLUMN d int;
SELECT * FROM test_alter_table;
INSERT INTO test_alter_table (SELECT 3, 5, 8);
SELECT * FROM test_alter_table;


-- add a fixed-length column with default value
ALTER FOREIGN TABLE test_alter_table ADD COLUMN e int default 3;
SELECT * from test_alter_table;
INSERT INTO test_alter_table (SELECT 1, 2, 4, 8);
SELECT * from test_alter_table;


-- add a variable-length column with default value
ALTER FOREIGN TABLE test_alter_table ADD COLUMN f text DEFAULT 'TEXT ME';
SELECT * from test_alter_table;
INSERT INTO test_alter_table (SELECT 1, 2, 4, 8, 'ABCDEF');
SELECT * from test_alter_table;


-- drop couple of columns
ALTER FOREIGN TABLE test_alter_table DROP COLUMN c;
ALTER FOREIGN TABLE test_alter_table DROP COLUMN e;
ANALYZE test_alter_table;
SELECT * from test_alter_table;
SELECT count(*) from test_alter_table;
SELECT count(t.*) from test_alter_table t;


-- unsupported default values
ALTER FOREIGN TABLE test_alter_table ADD COLUMN g boolean DEFAULT isfinite(current_date);
ALTER FOREIGN TABLE test_alter_table ADD COLUMN h DATE DEFAULT current_date;
SELECT * FROM test_alter_table;
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN g DROP DEFAULT;
SELECT * FROM test_alter_table;
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN h DROP DEFAULT;
ANALYZE test_alter_table;
SELECT * FROM test_alter_table;

-- unsupported type change
ALTER FOREIGN TABLE test_alter_table ADD COLUMN i int;
ALTER FOREIGN TABLE test_alter_table ADD COLUMN j float;
ALTER FOREIGN TABLE test_alter_table ADD COLUMN k text;

-- this is valid type change
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN i TYPE float;

-- this is not valid
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN j TYPE int;

-- text / varchar conversion is valid both ways
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN k TYPE varchar(20);
ALTER FOREIGN TABLE test_alter_table ALTER COLUMN k TYPE text;

DROP FOREIGN TABLE test_alter_table;
