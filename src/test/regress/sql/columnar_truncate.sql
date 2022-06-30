--
-- Test the TRUNCATE TABLE command for columnar tables.
--

-- CREATE a columnar table, fill with some data --
CREATE TABLE columnar_truncate_test (a int, b int) USING columnar;
CREATE TABLE columnar_truncate_test_second (a int, b int) USING columnar;
-- COMPRESSED
CREATE TABLE columnar_truncate_test_compressed (a int, b int) USING columnar;
CREATE TABLE columnar_truncate_test_regular (a int, b int);

SELECT count(distinct storage_id) AS columnar_data_files_before_truncate FROM columnar.stripe \gset

INSERT INTO columnar_truncate_test select a, a from generate_series(1, 10) a;

set columnar.compression = 'pglz';
INSERT INTO columnar_truncate_test_compressed select a, a from generate_series(1, 10) a;
INSERT INTO columnar_truncate_test_compressed select a, a from generate_series(1, 10) a;
set columnar.compression to default;

-- query rows
SELECT * FROM columnar_truncate_test;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('columnar_truncate_test');

TRUNCATE TABLE columnar_truncate_test;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('columnar_truncate_test');

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

SELECT * FROM columnar_truncate_test;

SELECT COUNT(*) from columnar_truncate_test;

SELECT count(*) FROM columnar_truncate_test_compressed;
TRUNCATE TABLE columnar_truncate_test_compressed;
SELECT count(*) FROM columnar_truncate_test_compressed;

SELECT pg_relation_size('columnar_truncate_test_compressed');

INSERT INTO columnar_truncate_test select a, a from generate_series(1, 10) a;
INSERT INTO columnar_truncate_test_regular select a, a from generate_series(10, 20) a;
INSERT INTO columnar_truncate_test_second select a, a from generate_series(20, 30) a;

SELECT * from columnar_truncate_test;

SELECT * from columnar_truncate_test_second;

SELECT * from columnar_truncate_test_regular;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

-- make sure multi truncate works
-- notice that the same table might be repeated
TRUNCATE TABLE columnar_truncate_test,
			   columnar_truncate_test_regular,
			   columnar_truncate_test_second,
   			   columnar_truncate_test;

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

SELECT * from columnar_truncate_test;
SELECT * from columnar_truncate_test_second;
SELECT * from columnar_truncate_test_regular;

-- test if truncate on empty table works
TRUNCATE TABLE columnar_truncate_test;
SELECT * from columnar_truncate_test;

-- make sure TRUNATE deletes metadata for old relfilenode
SELECT :columnar_data_files_before_truncate - count(distinct storage_id) FROM columnar.stripe;

-- test if truncation in the same transaction that created the table works properly
BEGIN;
CREATE TABLE columnar_same_transaction_truncate(a int) USING columnar;
INSERT INTO columnar_same_transaction_truncate SELECT * FROM generate_series(1, 100);
TRUNCATE columnar_same_transaction_truncate;
INSERT INTO columnar_same_transaction_truncate SELECT * FROM generate_series(20, 23);
COMMIT;

-- should output "1" for the newly created relation
SELECT count(distinct storage_id) - :columnar_data_files_before_truncate FROM columnar_internal.stripe;
SELECT * FROM columnar_same_transaction_truncate;

DROP TABLE columnar_same_transaction_truncate;

-- test if a cached truncate from a pl/pgsql function works
CREATE FUNCTION columnar_truncate_test_regular_func() RETURNS void AS $$
BEGIN
	INSERT INTO columnar_truncate_test_regular select a, a from generate_series(1, 10) a;
	TRUNCATE TABLE columnar_truncate_test_regular;
END;$$
LANGUAGE plpgsql;

SELECT columnar_truncate_test_regular_func();
-- the cached plans are used stating from the second call
SELECT columnar_truncate_test_regular_func();
DROP FUNCTION columnar_truncate_test_regular_func();

DROP TABLE columnar_truncate_test, columnar_truncate_test_second;
DROP TABLE columnar_truncate_test_regular;
DROP TABLE columnar_truncate_test_compressed;

-- test truncate with schema
CREATE SCHEMA truncate_schema;
-- COMPRESSED
CREATE TABLE truncate_schema.truncate_tbl (id int) USING columnar;
set columnar.compression = 'pglz';
INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);
set columnar.compression to default;
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

set columnar.compression = 'pglz';
INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);
set columnar.compression to default;
-- create a user that can not truncate
CREATE USER truncate_user;
GRANT USAGE ON SCHEMA truncate_schema TO truncate_user;
GRANT SELECT ON TABLE truncate_schema.truncate_tbl TO truncate_user;
REVOKE TRUNCATE ON TABLE truncate_schema.truncate_tbl FROM truncate_user;

SELECT current_user \gset

\c - truncate_user
-- verify truncate command fails and check number of rows
SELECT count(*) FROM truncate_schema.truncate_tbl;
TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT count(*) FROM truncate_schema.truncate_tbl;

-- switch to super user, grant truncate to truncate_user
\c - :current_user
GRANT TRUNCATE ON TABLE truncate_schema.truncate_tbl TO truncate_user;

-- verify truncate_user can truncate now
\c - truncate_user
SELECT count(*) FROM truncate_schema.truncate_tbl;
TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT count(*) FROM truncate_schema.truncate_tbl;
\c - :current_user

SELECT * FROM columnar_test_helpers.chunk_group_consistency;

-- cleanup
DROP SCHEMA truncate_schema CASCADE;
DROP USER truncate_user;
