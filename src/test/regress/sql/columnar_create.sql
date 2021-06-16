--
-- Test the CREATE statements related to columnar.
--


-- Create uncompressed table
CREATE TABLE contestant (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;
SELECT alter_columnar_table_set('contestant', compression => 'none');

CREATE INDEX contestant_idx on contestant(handle);

-- Create zstd compressed table
CREATE TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
	percentile FLOAT, country CHAR(3), achievements TEXT[])
	USING columnar;

-- Test that querying an empty table works
ANALYZE contestant;
SELECT count(*) FROM contestant;

-- Should fail: unlogged tables not supported
CREATE UNLOGGED TABLE columnar_unlogged(i int) USING columnar;

CREATE TABLE columnar_table_1 (a int) USING columnar;
INSERT INTO columnar_table_1 VALUES (1);

CREATE MATERIALIZED VIEW columnar_table_1_mv USING columnar
AS SELECT * FROM columnar_table_1;

SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_table_1_mv_storage_id
FROM pg_class WHERE relname='columnar_table_1_mv' \gset

-- test columnar_relation_set_new_filenode
REFRESH MATERIALIZED VIEW columnar_table_1_mv;
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_mv_storage_id);

SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_table_1_storage_id
FROM pg_class WHERE relname='columnar_table_1' \gset

BEGIN;
  -- test columnar_relation_nontransactional_truncate
  TRUNCATE columnar_table_1;
  SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);
ROLLBACK;

-- since we rollback'ed above xact, should return true
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);

-- test dropping columnar table
DROP TABLE columnar_table_1 CASCADE;
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);

-- test temporary columnar tables

-- Should work: temporary tables are supported
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO columnar_temp SELECT i FROM generate_series(1,5) i;

SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_temp_storage_id
FROM pg_class WHERE relname='columnar_temp' \gset

\c - - - :master_port

-- show that temporary table itself and it's metadata is removed
SELECT COUNT(*)=0 FROM pg_class WHERE relname='columnar_temp';
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

-- connect to another session and create a temp table with same name
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO columnar_temp SELECT i FROM generate_series(1,5) i;

-- test basic select
SELECT COUNT(*) FROM columnar_temp WHERE i < 5;

SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_temp_storage_id
FROM pg_class WHERE relname='columnar_temp' \gset

BEGIN;
  DROP TABLE columnar_temp;
  -- show that we drop stripes properly
  SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);
ROLLBACK;

-- make sure that table is not dropped yet since we rollbacked above xact
SELECT COUNT(*)=1 FROM pg_class WHERE relname='columnar_temp';
-- show that we preserve the stripe of the temp columanar table after rollback
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

-- drop it for next tests
DROP TABLE columnar_temp;

BEGIN;
  CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar ON COMMIT DROP;
  -- force flushing stripe
  INSERT INTO columnar_temp SELECT i FROM generate_series(1,150000) i;

  SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_temp_storage_id
  FROM pg_class WHERE relname='columnar_temp' \gset
COMMIT;

-- make sure that table & it's stripe is dropped after commiting above xact
SELECT COUNT(*)=0 FROM pg_class WHERE relname='columnar_temp';
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

BEGIN;
  CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar ON COMMIT DELETE ROWS;
  -- force flushing stripe
  INSERT INTO columnar_temp SELECT i FROM generate_series(1,150000) i;

  SELECT columnar_test_helpers.columnar_relation_storageid(oid) AS columnar_temp_storage_id
  FROM pg_class WHERE relname='columnar_temp' \gset
COMMIT;

-- make sure that table is not dropped but it's rows's are deleted after commiting above xact
SELECT COUNT(*)=1 FROM pg_class WHERE relname='columnar_temp';
SELECT COUNT(*)=0 FROM columnar_temp;
-- since we deleted all the rows, we shouldn't have any stripes for table
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);
