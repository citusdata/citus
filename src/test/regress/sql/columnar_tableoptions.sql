CREATE SCHEMA am_tableoptions;
SET search_path TO am_tableoptions;
SET columnar.compression TO 'none';

CREATE TABLE table_options (a int) USING columnar;
INSERT INTO table_options SELECT generate_series(1,100);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- test changing the compression
ALTER TABLE table_options SET (columnar.compression = pglz);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- test changing the compression level
ALTER TABLE table_options SET (columnar.compression_level = 5);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- test changing the chunk_group_row_limit
ALTER TABLE table_options SET (columnar.chunk_group_row_limit = 2000);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- test changing the chunk_group_row_limit
ALTER TABLE table_options SET (columnar.stripe_row_limit = 4000);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- VACUUM FULL creates a new table, make sure it copies settings from the table you are vacuuming
VACUUM FULL table_options;

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- set all settings at the same time
ALTER TABLE table_options SET
  (columnar.stripe_row_limit = 8000,
   columnar.chunk_group_row_limit = 4000,
   columnar.compression = none,
   columnar.compression_level = 7);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- make sure table options are not changed when VACUUM a table
VACUUM table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- make sure table options are not changed when VACUUM FULL a table
VACUUM FULL table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- make sure table options are not changed when truncating a table
TRUNCATE table_options;
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options ALTER COLUMN a TYPE bigint;
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- reset settings one by one to the version of the GUC's
SET columnar.chunk_group_row_limit TO 1000;
SET columnar.stripe_row_limit TO 10000;
SET columnar.compression TO 'pglz';
SET columnar.compression_level TO 11;

-- verify setting the GUC's didn't change the settings
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options RESET (columnar.chunk_group_row_limit);
-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options RESET (columnar.stripe_row_limit);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options RESET (columnar.compression);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options RESET (columnar.compression_level);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- verify resetting all settings at once work
SET columnar.chunk_group_row_limit TO 10000;
SET columnar.stripe_row_limit TO 100000;
SET columnar.compression TO 'none';
SET columnar.compression_level TO 13;

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options RESET
  (columnar.chunk_group_row_limit,
   columnar.stripe_row_limit,
   columnar.compression,
   columnar.compression_level);

-- show table_options settings
SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- verify edge cases
-- first start with a table that is not a columnar table
CREATE TABLE not_a_columnar_table (a int);
ALTER TABLE not_a_columnar_table SET (columnar.compression = pglz);
ALTER TABLE not_a_columnar_table RESET (columnar.compression);

-- verify you can't use a compression that is not known
ALTER TABLE table_options SET (columnar.compression = foobar);

-- verify you can't use a columnar setting that is not known
ALTER TABLE table_options SET (columnar.foobar = 123);
ALTER TABLE table_options RESET (columnar.foobar);

-- verify that invalid options are caught early, before query executes
-- (error should be about invalid options, not division-by-zero)
CREATE TABLE fail(i) USING columnar WITH (columnar.foobar = 123) AS SELECT 1/0;
CREATE TABLE fail(i) USING columnar WITH (columnar.compression = foobar) AS SELECT 1/0;

-- verify cannot set out of range compression levels
ALTER TABLE table_options SET (columnar.compression_level = 0);
ALTER TABLE table_options SET (columnar.compression_level = 20);

-- verify cannot set out of range stripe_row_limit & chunk_group_row_limit options
ALTER TABLE table_options SET (columnar.stripe_row_limit = 999);
ALTER TABLE table_options SET (columnar.stripe_row_limit = 10000001);
ALTER TABLE table_options SET (columnar.chunk_group_row_limit = 999);
ALTER TABLE table_options SET (columnar.chunk_group_row_limit = 100001);
ALTER TABLE table_options SET (columnar.chunk_group_row_limit = 0);
INSERT INTO table_options VALUES (1);

-- multiple SET/RESET clauses
ALTER TABLE table_options
  SET (columnar.compression = pglz, columnar.compression_level = 7),
  SET (columnar.compression_level = 6);

SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

ALTER TABLE table_options
  SET (columnar.compression = pglz, columnar.stripe_row_limit = 7777),
  RESET (columnar.stripe_row_limit),
  SET (columnar.chunk_group_row_limit = 5555);

SELECT * FROM columnar.options
WHERE relation = 'table_options'::regclass;

-- a no-op; shouldn't throw an error
ALTER TABLE IF EXISTS what SET (columnar.compression = lz4);

-- a no-op; shouldn't throw an error
CREATE TABLE IF NOT EXISTS table_options(a int) USING columnar
  WITH (columnar.compression_level = 4);

-- test old interface based on functions
SELECT alter_columnar_table_reset('table_options', compression => true);
SELECT * FROM columnar.options WHERE relation = 'table_options'::regclass;
SELECT alter_columnar_table_set('table_options', compression_level => 1);
SELECT * FROM columnar.options WHERE relation = 'table_options'::regclass;

-- error: set columnar options on heap tables
CREATE TABLE heap_options(i int) USING heap;
ALTER TABLE heap_options SET (columnar.stripe_row_limit = 12000);

-- ordinarily, postgres allows bogus options for a RESET clause,
-- but if it's a heap table and someone specifies columnar options,
-- we block them
ALTER TABLE heap_options RESET (columnar.stripe_row_limit, foobar);
DROP TABLE heap_options;

-- verify options are removed when table is dropped
DROP TABLE table_options;
-- we expect no entries in çstore.options for anything not found int pg_class
SELECT * FROM columnar.options o WHERE o.relation NOT IN (SELECT oid FROM pg_class);

SET client_min_messages TO warning;
DROP SCHEMA am_tableoptions CASCADE;
