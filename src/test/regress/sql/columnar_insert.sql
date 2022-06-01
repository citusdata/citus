--
-- Testing insert on columnar tables.
--

CREATE SCHEMA columnar_insert;
SET search_path TO columnar_insert;

CREATE TABLE test_insert_command (a int) USING columnar;

-- test single row inserts fail
select count(*) from test_insert_command;
insert into test_insert_command values(1);
select count(*) from test_insert_command;

insert into test_insert_command default values;
select count(*) from test_insert_command;

-- test inserting from another table succeed
CREATE TABLE test_insert_command_data (a int);

select count(*) from test_insert_command_data;
insert into test_insert_command_data values(1);
select count(*) from test_insert_command_data;

insert into test_insert_command select * from test_insert_command_data;
select count(*) from test_insert_command;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('test_insert_command');

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

drop table test_insert_command_data;
drop table test_insert_command;

-- test long attribute value insertion
-- create sufficiently long text so that data is stored in toast
CREATE TABLE test_long_text AS
SELECT a as int_val, string_agg(random()::text, '') as text_val
FROM generate_series(1, 10) a, generate_series(1, 1000) b
GROUP BY a ORDER BY a;

-- store hash values of text for later comparison
CREATE TABLE test_long_text_hash AS
SELECT int_val, md5(text_val) AS hash
FROM test_long_text;

CREATE TABLE test_columnar_long_text(int_val int, text_val text)
USING columnar;

-- store long text in columnar table
INSERT INTO test_columnar_long_text SELECT * FROM test_long_text;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

-- drop source table to remove original text from toast
DROP TABLE test_long_text;

-- check if text data is still available in columnar table
-- by comparing previously stored hash.
SELECT a.int_val
FROM  test_long_text_hash a, test_columnar_long_text c
WHERE a.int_val = c.int_val AND a.hash = md5(c.text_val);

DROP TABLE test_long_text_hash;
DROP TABLE test_columnar_long_text;

CREATE TABLE test_logical_replication(i int) USING columnar;
-- should succeed
INSERT INTO test_logical_replication VALUES (1);
CREATE PUBLICATION test_columnar_publication
  FOR TABLE test_logical_replication;
-- should fail; columnar does not support logical replication
INSERT INTO test_logical_replication VALUES (2);
DROP PUBLICATION test_columnar_publication;
-- should succeed
INSERT INTO test_logical_replication VALUES (3);
DROP TABLE test_logical_replication;

--
-- test toast interactions
--

-- row table with data in different storage formats
CREATE TABLE test_toast_row(plain TEXT, main TEXT, external TEXT, extended TEXT);
ALTER TABLE test_toast_row ALTER COLUMN plain SET STORAGE plain; -- inline, uncompressed
ALTER TABLE test_toast_row ALTER COLUMN main SET STORAGE main; -- inline, compressed
ALTER TABLE test_toast_row ALTER COLUMN external SET STORAGE external; -- out-of-line, uncompressed
ALTER TABLE test_toast_row ALTER COLUMN extended SET STORAGE extended; -- out-of-line, compressed

INSERT INTO test_toast_row VALUES(
       repeat('w', 5000), repeat('x', 5000), repeat('y', 5000), repeat('z', 5000));

SELECT
  pg_column_size(plain), pg_column_size(main),
  pg_column_size(external), pg_column_size(extended)
FROM test_toast_row;

CREATE TABLE test_toast_columnar(plain TEXT, main TEXT, external TEXT, extended TEXT)
  USING columnar;
INSERT INTO test_toast_columnar SELECT plain, main, external, extended
  FROM test_toast_row;
SELECT
  pg_column_size(plain), pg_column_size(main),
  pg_column_size(external), pg_column_size(extended)
FROM test_toast_columnar;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('test_toast_columnar');

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

DROP TABLE test_toast_row;
DROP TABLE test_toast_columnar;

-- Verify metadata for zero column tables.
-- We support writing into zero column tables, but not reading from them.
-- We test that metadata makes sense so we can fix the read path in future.
CREATE TABLE zero_col() USING columnar;
ALTER TABLE zero_col SET (columnar.chunk_group_row_limit = 1000);

INSERT INTO zero_col DEFAULT VALUES;
INSERT INTO zero_col DEFAULT VALUES;
INSERT INTO zero_col DEFAULT VALUES;
INSERT INTO zero_col DEFAULT VALUES;

CREATE TABLE zero_col_heap();
INSERT INTO zero_col_heap DEFAULT VALUES;
INSERT INTO zero_col_heap DEFAULT VALUES;
INSERT INTO zero_col_heap DEFAULT VALUES;
INSERT INTO zero_col_heap DEFAULT VALUES;

INSERT INTO zero_col_heap SELECT * FROM zero_col_heap;
INSERT INTO zero_col_heap SELECT * FROM zero_col_heap;
INSERT INTO zero_col_heap SELECT * FROM zero_col_heap;
INSERT INTO zero_col_heap SELECT * FROM zero_col_heap;

INSERT INTO zero_col SELECT * FROM zero_col_heap;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('zero_col');

SELECT relname, stripe_num, chunk_group_count, row_count FROM columnar.stripe a, pg_class b
WHERE columnar.get_storage_id(b.oid)=a.storage_id AND relname = 'zero_col'
ORDER BY 1,2,3,4;

SELECT relname, stripe_num, value_count FROM columnar.chunk a, pg_class b
WHERE columnar.get_storage_id(b.oid)=a.storage_id AND relname = 'zero_col'
ORDER BY 1,2,3;

SELECT relname, stripe_num, chunk_group_num, row_count FROM columnar.chunk_group a, pg_class b
WHERE columnar.get_storage_id(b.oid)=a.storage_id AND relname = 'zero_col'
ORDER BY 1,2,3,4;

CREATE TABLE selfinsert(x int) USING columnar;

ALTER TABLE selfinsert SET (columnar.stripe_row_limit = 1000);

BEGIN;
  INSERT INTO selfinsert SELECT generate_series(1,1010);
  INSERT INTO selfinsert SELECT * FROM selfinsert;

  SELECT SUM(x)=1021110 FROM selfinsert;
ROLLBACK;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
  INSERT INTO selfinsert SELECT generate_series(1,1010);
  INSERT INTO selfinsert SELECT * FROM selfinsert;

  SELECT SUM(x)=1021110 FROM selfinsert;
ROLLBACK;

INSERT INTO selfinsert SELECT generate_series(1,1010);
INSERT INTO selfinsert SELECT * FROM selfinsert;

SELECT SUM(x)=1021110 FROM selfinsert;

CREATE TABLE selfconflict (f1 int PRIMARY KEY, f2 int) USING columnar;

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;
  INSERT INTO selfconflict VALUES (2,1), (2,2);
COMMIT;

SELECT COUNT(*)=0 FROM selfconflict;

CREATE TABLE flush_create_index(a int, b int) USING columnar;
BEGIN;
  INSERT INTO flush_create_index VALUES (5, 10);

  SET enable_seqscan TO OFF;
  SET columnar.enable_custom_scan TO OFF;
  SET enable_indexscan TO ON;

  CREATE INDEX ON flush_create_index(a);

  SELECT a FROM flush_create_index WHERE a=5;
ROLLBACK;

CREATE OR REPLACE FUNCTION test_columnar_storage_write_new_page(relation regclass) RETURNS void
STRICT LANGUAGE c AS 'citus', 'test_columnar_storage_write_new_page';

CREATE TABLE aborted_write (a int, b int) USING columnar;

SELECT test_columnar_storage_write_new_page('aborted_write');

SET client_min_messages TO DEBUG4;
INSERT INTO aborted_write VALUES (5);

RESET search_path;
SET client_min_messages TO WARNING;
DROP SCHEMA columnar_insert CASCADE;

