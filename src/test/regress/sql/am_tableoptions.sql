CREATE SCHEMA am_tableoptions;
SET search_path TO am_tableoptions;
SET columnar.compression TO 'none';

CREATE TABLE table_options (a int) USING columnar;
INSERT INTO table_options SELECT generate_series(1,100);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- test changing the compression
SELECT alter_columnar_table_set('table_options', compression => 'pglz');

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- test changing the compression level
SELECT alter_columnar_table_set('table_options', compression_level => 5);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- test changing the chunk_row_count
SELECT alter_columnar_table_set('table_options', chunk_row_count => 10);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- test changing the chunk_row_count
SELECT alter_columnar_table_set('table_options', stripe_row_count => 100);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- VACUUM FULL creates a new table, make sure it copies settings from the table you are vacuuming
VACUUM FULL table_options;

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- set all settings at the same time
SELECT alter_columnar_table_set('table_options', stripe_row_count => 1000, chunk_row_count => 100, compression => 'none', compression_level => 7);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- make sure table options are not changed when VACUUM a table
VACUUM table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- make sure table options are not changed when VACUUM FULL a table
VACUUM FULL table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- make sure table options are not changed when truncating a table
TRUNCATE table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

ALTER TABLE table_options ALTER COLUMN a TYPE bigint;
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- reset settings one by one to the version of the GUC's
SET columnar.chunk_row_count TO 1000;
SET columnar.stripe_row_count TO 10000;
SET columnar.compression TO 'pglz';
SET columnar.compression_level TO 11;

-- verify setting the GUC's didn't change the settings
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

SELECT alter_columnar_table_reset('table_options', chunk_row_count => true);
-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

SELECT alter_columnar_table_reset('table_options', stripe_row_count => true);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

SELECT alter_columnar_table_reset('table_options', compression => true);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

SELECT alter_columnar_table_reset('table_options', compression_level => true);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- verify resetting all settings at once work
SET columnar.chunk_row_count TO 10000;
SET columnar.stripe_row_count TO 100000;
SET columnar.compression TO 'none';
SET columnar.compression_level TO 13;

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

SELECT alter_columnar_table_reset(
    'table_options',
    chunk_row_count => true,
    stripe_row_count => true,
    compression => true,
    compression_level => true);

-- show table_options settings
SELECT * FROM columnar.options
WHERE regclass = 'table_options'::regclass;

-- verify edge cases
-- first start with a table that is not a columnar table
CREATE TABLE not_a_columnar_table (a int);
SELECT alter_columnar_table_set('not_a_columnar_table', compression => 'pglz');
SELECT alter_columnar_table_reset('not_a_columnar_table', compression => true);

-- verify you can't use a compression that is not known
SELECT alter_columnar_table_set('table_options', compression => 'foobar');

-- verify cannot set out of range compression levels
SELECT alter_columnar_table_set('table_options', compression_level => 0);
SELECT alter_columnar_table_set('table_options', compression_level => 20);

-- verify options are removed when table is dropped
DROP TABLE table_options;
-- we expect no entries in çstore.options for anything not found int pg_class
SELECT * FROM columnar.options o WHERE o.regclass NOT IN (SELECT oid FROM pg_class);

SET client_min_messages TO warning;
DROP SCHEMA am_tableoptions CASCADE;
