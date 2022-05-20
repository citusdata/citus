--
-- Testing we handle rollbacks properly
--

CREATE TABLE t(a int, b int) USING columnar;

CREATE VIEW t_stripes AS
SELECT * FROM columnar.stripe a, pg_class b
WHERE a.storage_id = columnar.get_storage_id(b.oid) AND b.relname = 't';

BEGIN;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
ROLLBACK;
SELECT count(*) FROM t;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

-- check stripe metadata also have been rolled-back
SELECT count(*) FROM t_stripes;

INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
SELECT count(*) FROM t;

SELECT count(*) FROM t_stripes;

-- savepoint rollback
BEGIN;
SAVEPOINT s0;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
SELECT count(*) FROM t;  -- force flush
SAVEPOINT s1;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s1;
SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s0;
SELECT count(*) FROM t;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
COMMIT;

select
  version_major, version_minor, reserved_stripe_id, reserved_row_number
  from columnar_test_helpers.columnar_storage_info('t');

SELECT count(*) FROM t;

SELECT count(*) FROM t_stripes;

DROP TABLE t;
DROP VIEW t_stripes;
