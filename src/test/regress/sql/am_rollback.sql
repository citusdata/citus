--
-- Testing we handle rollbacks properly
--

CREATE TABLE t(a int, b int) USING columnar;

CREATE VIEW t_stripes AS
SELECT * FROM columnar.columnar_stripes a, pg_class b
WHERE a.storageid = columnar_relation_storageid(b.oid) AND b.relname = 't';

BEGIN;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
ROLLBACK;
SELECT count(*) FROM t;

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
SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s1;
SELECT count(*) FROM t;
ROLLBACK TO SAVEPOINT s0;
SELECT count(*) FROM t;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
COMMIT;

SELECT count(*) FROM t;

SELECT count(*) FROM t_stripes;

DROP TABLE t;
DROP VIEW t_stripes;
