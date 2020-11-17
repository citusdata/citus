--
-- Testing we handle rollbacks properly
--

CREATE TABLE t(a int, b int) USING cstore_tableam;

BEGIN;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
ROLLBACK;
SELECT count(*) FROM t;

-- check stripe metadata also have been rolled-back
SELECT count(*) FROM cstore.cstore_stripes a, pg_class b
WHERE a.relfilenode = b.relfilenode AND b.relname = 't';

INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
SELECT count(*) FROM t;

SELECT count(*) FROM cstore.cstore_stripes a, pg_class b
WHERE a.relfilenode = b.relfilenode AND b.relname = 't';

-- savepoint rollback
BEGIN;
SAVEPOINT s0;
INSERT INTO t SELECT i, i+1 FROM generate_series(1, 10) i;
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

SELECT count(*) FROM cstore.cstore_stripes a, pg_class b
WHERE a.relfilenode = b.relfilenode AND b.relname = 't';

DROP TABLE t;
