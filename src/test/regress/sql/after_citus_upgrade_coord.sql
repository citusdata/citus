SET search_path TO before_citus_upgrade_coord, public;

SELECT * FROM t ORDER BY a;
SELECT * FROM t WHERE a = 1;

INSERT INTO t SELECT * FROM generate_series(10, 15);

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;

TRUNCATE TABLE t;

SELECT * FROM T;

DROP TABLE t;

DROP SCHEMA before_citus_upgrade_coord CASCADE;