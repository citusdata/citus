CREATE SCHEMA columnar_first_row_number;
SET search_path tO columnar_first_row_number;

CREATE TABLE col_table_1 (a int) USING columnar;

INSERT INTO col_table_1 SELECT i FROM generate_series(1, 10) i;

BEGIN;
  -- we don't use same first_row_number even if the xact is rollback'ed
  INSERT INTO col_table_1 SELECT i FROM generate_series(1, 11) i;
ROLLBACK;

INSERT INTO col_table_1 SELECT i FROM generate_series(1, 12) i;

SELECT alter_columnar_table_set('col_table_1', stripe_row_limit => 1000);

INSERT INTO col_table_1 SELECT i FROM generate_series(1, 2350) i;

SELECT row_count, first_row_number FROM columnar.stripe a
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid('col_table_1'::regclass)
ORDER BY stripe_num;

VACUUM FULL col_table_1;

-- show that we properly update first_row_number after VACUUM FULL
SELECT row_count, first_row_number FROM columnar.stripe a
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid('col_table_1'::regclass)
ORDER BY stripe_num;

TRUNCATE col_table_1;

BEGIN;
  INSERT INTO col_table_1 SELECT i FROM generate_series(1, 16) i;
  INSERT INTO col_table_1 SELECT i FROM generate_series(1, 16) i;
COMMIT;

-- show that we start with first_row_number=1 after TRUNCATE
SELECT row_count, first_row_number FROM columnar.stripe a
WHERE a.storage_id = columnar_test_helpers.columnar_relation_storageid('col_table_1'::regclass)
ORDER BY stripe_num;

SET client_min_messages TO ERROR;
DROP SCHEMA columnar_first_row_number CASCADE;
