--
-- Test the CREATE statements related to columnar.
--

-- We cannot create below tables within columnar_create because columnar_create
-- is dropped at the end of this test but unfortunately some other tests depend
-- those tables too.
--
-- However, this file has to be runnable multiple times for flaky test detection;
-- so we create them below --outside columnar_create-- idempotantly.
DO
$$
BEGIN
IF NOT EXISTS (
    SELECT 1 FROM pg_class
    WHERE relname = 'contestant' AND
          relnamespace = (
            SELECT oid FROM pg_namespace WHERE nspname = 'public'
          )
   )
THEN
    -- Create uncompressed table
    CREATE TABLE contestant (handle TEXT, birthdate DATE, rating INT,
        percentile FLOAT, country CHAR(3), achievements TEXT[])
        USING columnar;
    ALTER TABLE contestant SET (columnar.compression = none);

    CREATE INDEX contestant_idx on contestant(handle);

    -- Create zstd compressed table
    CREATE TABLE contestant_compressed (handle TEXT, birthdate DATE, rating INT,
        percentile FLOAT, country CHAR(3), achievements TEXT[])
        USING columnar;

    -- Test that querying an empty table works
    ANALYZE contestant;
END IF;
END
$$
LANGUAGE plpgsql;

SELECT count(*) FROM contestant;

CREATE SCHEMA columnar_create;
SET search_path TO columnar_create;

-- Should fail: unlogged tables not supported
CREATE UNLOGGED TABLE columnar_unlogged(i int) USING columnar;

CREATE TABLE columnar_table_1 (a int) USING columnar;
INSERT INTO columnar_table_1 VALUES (1);

CREATE MATERIALIZED VIEW columnar_table_1_mv USING columnar
AS SELECT * FROM columnar_table_1;

SELECT columnar.get_storage_id(oid) AS columnar_table_1_mv_storage_id
FROM pg_class WHERE relname='columnar_table_1_mv' \gset

-- test columnar_relation_set_new_filelocator
REFRESH MATERIALIZED VIEW columnar_table_1_mv;
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_mv_storage_id);

SELECT columnar.get_storage_id(oid) AS columnar_table_1_storage_id
FROM pg_class WHERE relname='columnar_table_1' \gset

BEGIN;
  -- test columnar_relation_nontransactional_truncate
  TRUNCATE columnar_table_1;
  SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);
ROLLBACK;

-- since we rollback'ed above xact, should return true
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);

BEGIN;
  INSERT INTO columnar_table_1 VALUES (2);
ROLLBACK;

INSERT INTO columnar_table_1 VALUES (3),(4);
INSERT INTO columnar_table_1 VALUES (5),(6);
INSERT INTO columnar_table_1 VALUES (7),(8);

-- Test whether columnar metadata accessors are still fine even
-- when the metadata indexes are not available to them.
BEGIN;
  ALTER INDEX columnar_internal.stripe_first_row_number_idx RENAME TO new_index_name;
  ALTER INDEX columnar_internal.chunk_pkey RENAME TO new_index_name_1;
  ALTER INDEX columnar_internal.stripe_pkey RENAME TO new_index_name_2;
  ALTER INDEX columnar_internal.chunk_group_pkey RENAME TO new_index_name_3;

  CREATE INDEX columnar_table_1_idx ON columnar_table_1(a);

  -- make sure that we test index scan
  SET LOCAL columnar.enable_custom_scan TO 'off';
  SET LOCAL enable_seqscan TO off;
  SET LOCAL seq_page_cost TO 10000000;

  SELECT * FROM columnar_table_1 WHERE a = 6;
  SELECT * FROM columnar_table_1 WHERE a = 5;
  SELECT * FROM columnar_table_1 WHERE a = 7;
  SELECT * FROM columnar_table_1 WHERE a = 3;

  DROP INDEX columnar_table_1_idx;

  -- Re-shuffle some metadata records to test whether we can
  -- rely on sequential metadata scan when the metadata records
  -- are not ordered by their "first_row_number"s.
  WITH cte AS (
      DELETE FROM columnar_internal.stripe
      WHERE storage_id = columnar.get_storage_id('columnar_table_1')
      RETURNING *
  )
  INSERT INTO columnar_internal.stripe SELECT * FROM cte ORDER BY first_row_number DESC;

  SELECT SUM(a) FROM columnar_table_1;

  SELECT * FROM columnar_table_1 WHERE a = 6;

  -- Run a SELECT query after the INSERT command to force flushing the
  -- data within the xact block.
  INSERT INTO columnar_table_1 VALUES (20);
  SELECT COUNT(*) FROM columnar_table_1;

  DROP TABLE columnar_table_1 CASCADE;
ROLLBACK;

-- test dropping columnar table
DROP TABLE columnar_table_1 CASCADE;
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_table_1_storage_id);

-- test temporary columnar tables

-- Should work: temporary tables are supported
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO columnar_temp SELECT i FROM generate_series(1,5) i;

SELECT columnar.get_storage_id(oid) AS columnar_temp_storage_id
FROM pg_class WHERE relname='columnar_temp' \gset

SELECT pg_backend_pid() AS val INTO old_backend_pid;

\c - - - :master_port
SET search_path TO columnar_create;

-- wait until old backend to expire to make sure that temp table cleanup is complete
SELECT columnar_test_helpers.pg_waitpid(val) FROM old_backend_pid;

DROP TABLE old_backend_pid;

-- show that temporary table itself and its metadata is removed
SELECT COUNT(*)=0 FROM pg_class WHERE relname='columnar_temp';
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

-- connect to another session and create a temp table with same name
CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar;

-- reserve some chunks and a stripe
INSERT INTO columnar_temp SELECT i FROM generate_series(1,5) i;

-- test basic select
SELECT COUNT(*) FROM columnar_temp WHERE i < 5;

SELECT columnar.get_storage_id(oid) AS columnar_temp_storage_id
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

  SELECT columnar.get_storage_id(oid) AS columnar_temp_storage_id
  FROM pg_class WHERE relname='columnar_temp' \gset
COMMIT;

-- make sure that table & its stripe is dropped after commiting above xact
SELECT COUNT(*)=0 FROM pg_class WHERE relname='columnar_temp';
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

BEGIN;
  CREATE TEMPORARY TABLE columnar_temp(i int) USING columnar ON COMMIT DELETE ROWS;
  -- force flushing stripe
  INSERT INTO columnar_temp SELECT i FROM generate_series(1,150000) i;

  SELECT columnar.get_storage_id(oid) AS columnar_temp_storage_id
  FROM pg_class WHERE relname='columnar_temp' \gset
COMMIT;

-- make sure that table is not dropped but its rows's are deleted after commiting above xact
SELECT COUNT(*)=1 FROM pg_class WHERE relname='columnar_temp';
SELECT COUNT(*)=0 FROM columnar_temp;
-- since we deleted all the rows, we shouldn't have any stripes for table
SELECT columnar_test_helpers.columnar_metadata_has_storage_id(:columnar_temp_storage_id);

-- make sure citus_columnar can be loaded
LOAD 'citus_columnar';

SET client_min_messages TO WARNING;
DROP SCHEMA columnar_create CASCADE;
