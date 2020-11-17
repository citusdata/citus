--
-- Testing we handle transactions properly
--

CREATE TABLE t(a int, b int) USING cstore_tableam;

INSERT INTO t SELECT i, 2 * i FROM generate_series(1, 3) i;
SELECT * FROM t ORDER BY a;

-- verify that table rewrites work properly
BEGIN;
ALTER TABLE t ALTER COLUMN b TYPE float4 USING (b + 0.5)::float4;
INSERT INTO t VALUES (4, 8.5);
SELECT * FROM t ORDER BY a;
ROLLBACK;

SELECT * FROM t ORDER BY a;

-- verify truncate rollback
BEGIN;
TRUNCATE t;
INSERT INTO t VALUES (4, 8);
SELECT * FROM t ORDER BY a;
SAVEPOINT s1;
TRUNCATE t;
SELECT * FROM t ORDER BY a;
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM t ORDER BY a;
ROLLBACK;

-- verify truncate with unflushed data in upper xacts
BEGIN;
INSERT INTO t VALUES (4, 8);
SAVEPOINT s1;
TRUNCATE t;
ROLLBACK TO SAVEPOINT s1;
COMMIT;

SELECT * FROM t ORDER BY a;

-- verify DROP TABLE rollback
BEGIN;
INSERT INTO t VALUES (5, 10);
SELECT * FROM t ORDER BY a;
SAVEPOINT s1;
DROP TABLE t;
SELECT * FROM t ORDER BY a;
ROLLBACK TO SAVEPOINT s1;
SELECT * FROM t ORDER BY a;
ROLLBACK;

-- verify DROP TABLE with unflushed data in upper xacts
BEGIN;
INSERT INTO t VALUES (5, 10);
SAVEPOINT s1;
DROP TABLE t;
SELECT * FROM t ORDER BY a;
ROLLBACK TO SAVEPOINT s1;
COMMIT;
SELECT * FROM t ORDER BY a;

-- verify SELECT when unflushed data in upper transactions errors.
BEGIN;
INSERT INTO t VALUES (6, 12);
SAVEPOINT s1;
SELECT * FROM t;
ROLLBACK;
SELECT * FROM t ORDER BY a;

DROP TABLE t;
