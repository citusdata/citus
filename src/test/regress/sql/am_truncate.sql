--
-- Test the TRUNCATE TABLE command for cstore_fdw tables.
--

-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

-- CREATE a cstore_fdw table, fill with some data --
CREATE TABLE cstore_truncate_test (a int, b int) USING cstore_tableam;
CREATE TABLE cstore_truncate_test_second (a int, b int) USING cstore_tableam;
-- COMPRESSED
CREATE TABLE cstore_truncate_test_compressed (a int, b int) USING cstore_tableam;
CREATE TABLE cstore_truncate_test_regular (a int, b int);

SELECT count(*) AS cstore_data_files_before_truncate FROM cstore.cstore_data_files \gset

INSERT INTO cstore_truncate_test select a, a from generate_series(1, 10) a;

set cstore.compression = 'pglz';
INSERT INTO cstore_truncate_test_compressed select a, a from generate_series(1, 10) a;
INSERT INTO cstore_truncate_test_compressed select a, a from generate_series(1, 10) a;
set cstore.compression to default;

-- query rows
SELECT * FROM cstore_truncate_test;

TRUNCATE TABLE cstore_truncate_test;

SELECT * FROM cstore_truncate_test;

SELECT COUNT(*) from cstore_truncate_test;

SELECT count(*) FROM cstore_truncate_test_compressed;
TRUNCATE TABLE cstore_truncate_test_compressed;
SELECT count(*) FROM cstore_truncate_test_compressed;

SELECT pg_relation_size('cstore_truncate_test_compressed');

INSERT INTO cstore_truncate_test select a, a from generate_series(1, 10) a;
INSERT INTO cstore_truncate_test_regular select a, a from generate_series(10, 20) a;
INSERT INTO cstore_truncate_test_second select a, a from generate_series(20, 30) a;

SELECT * from cstore_truncate_test;

SELECT * from cstore_truncate_test_second;

SELECT * from cstore_truncate_test_regular;

-- make sure multi truncate works
-- notice that the same table might be repeated
TRUNCATE TABLE cstore_truncate_test,
			   cstore_truncate_test_regular,
			   cstore_truncate_test_second,
   			   cstore_truncate_test;

SELECT * from cstore_truncate_test;
SELECT * from cstore_truncate_test_second;
SELECT * from cstore_truncate_test_regular;

-- test if truncate on empty table works
TRUNCATE TABLE cstore_truncate_test;
SELECT * from cstore_truncate_test;

-- make sure TRUNATE deletes metadata for old relfilenode
SELECT :cstore_data_files_before_truncate - count(*) FROM cstore.cstore_data_files;

-- test if truncation in the same transaction that created the table works properly
BEGIN;
CREATE TABLE cstore_same_transaction_truncate(a int) USING cstore_tableam;
INSERT INTO cstore_same_transaction_truncate SELECT * FROM generate_series(1, 100);
TRUNCATE cstore_same_transaction_truncate;
INSERT INTO cstore_same_transaction_truncate SELECT * FROM generate_series(20, 23);
COMMIT;

-- should output "1" for the newly created relation
SELECT count(*) - :cstore_data_files_before_truncate FROM cstore.cstore_data_files;
SELECT * FROM cstore_same_transaction_truncate;

DROP TABLE cstore_same_transaction_truncate;

-- test if a cached truncate from a pl/pgsql function works
CREATE FUNCTION cstore_truncate_test_regular_func() RETURNS void AS $$
BEGIN
	INSERT INTO cstore_truncate_test_regular select a, a from generate_series(1, 10) a;
	TRUNCATE TABLE cstore_truncate_test_regular;
END;$$
LANGUAGE plpgsql;

SELECT cstore_truncate_test_regular_func();
-- the cached plans are used stating from the second call
SELECT cstore_truncate_test_regular_func();
DROP FUNCTION cstore_truncate_test_regular_func();

DROP TABLE cstore_truncate_test, cstore_truncate_test_second;
DROP TABLE cstore_truncate_test_regular;
DROP TABLE cstore_truncate_test_compressed;

-- test truncate with schema
CREATE SCHEMA truncate_schema;
-- COMPRESSED
CREATE TABLE truncate_schema.truncate_tbl (id int) USING cstore_tableam;
set cstore.compression = 'pglz';
INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);
set cstore.compression to default;
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

TRUNCATE TABLE truncate_schema.truncate_tbl;
SELECT COUNT(*) FROM truncate_schema.truncate_tbl;

set cstore.compression = 'pglz';
INSERT INTO truncate_schema.truncate_tbl SELECT generate_series(1, 100);
set cstore.compression to default;
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

-- cleanup
DROP SCHEMA truncate_schema CASCADE;
DROP USER truncate_user;
