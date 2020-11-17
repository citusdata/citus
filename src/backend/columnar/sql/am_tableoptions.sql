CREATE SCHEMA am_tableoptions;
SET search_path TO am_tableoptions;

CREATE TABLE table_options (a int) USING cstore_tableam;
INSERT INTO table_options SELECT generate_series(1,100);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- test changing the compression
SELECT alter_cstore_table_set('table_options', compression => 'pglz');

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- test changing the block_row_count
SELECT alter_cstore_table_set('table_options', block_row_count => 10);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- test changing the block_row_count
SELECT alter_cstore_table_set('table_options', stripe_row_count => 100);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- VACUUM FULL creates a new table, make sure it copies settings from the table you are vacuuming
VACUUM FULL table_options;

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- set all settings at the same time
SELECT alter_cstore_table_set('table_options', stripe_row_count => 1000, block_row_count => 100, compression => 'none');

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- reset settings one by one to the version of the GUC's
SET cstore.block_row_count TO 1000;
SET cstore.stripe_row_count TO 10000;
SET cstore.compression TO 'pglz';

-- verify setting the GUC's didn't change the settings
-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

SELECT alter_cstore_table_reset('table_options', block_row_count => true);
-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

SELECT alter_cstore_table_reset('table_options', stripe_row_count => true);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

SELECT alter_cstore_table_reset('table_options', compression => true);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- verify resetting all settings at once work
SET cstore.block_row_count TO 10000;
SET cstore.stripe_row_count TO 100000;
SET cstore.compression TO 'none';

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

SELECT alter_cstore_table_reset(
    'table_options',
    block_row_count => true,
    stripe_row_count => true,
    compression => true);

-- show table_options settings
SELECT * FROM cstore.cstore_options
WHERE regclass = 'table_options'::regclass;

-- verify edge cases
-- first start with a table that is not a cstore table
CREATE TABLE not_a_cstore_table (a int);
SELECT alter_cstore_table_set('not_a_cstore_table', compression => 'pglz');
SELECT alter_cstore_table_reset('not_a_cstore_table', compression => true);

-- verify you can't use a compression that is not known
SELECT alter_cstore_table_set('table_options', compression => 'foobar');

SET client_min_messages TO warning;
DROP SCHEMA am_tableoptions CASCADE;
