SET search_path TO upgrade_distributed_table_before, public;

SELECT * FROM t ORDER BY a;
SELECT * FROM t WHERE a = 1;

INSERT INTO t SELECT * FROM generate_series(10, 15);

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;

-- test distributed type
INSERT INTO t1 VALUES (1, (2,3)::tc1);
SELECT * FROM t1;
ALTER TYPE tc1 RENAME TO tc1_newname;
INSERT INTO t1 VALUES (3, (4,5)::tc1_newname);

TRUNCATE TABLE t;

SELECT * FROM T;

-- verify that the table whose column is dropped before a pg_upgrade still works as expected.
SELECT * FROM t_ab ORDER BY b;
SELECT * FROM t_ab WHERE b = 11;
SELECT * FROM t_ab WHERE b = 22;

DROP TABLE t;

DROP SCHEMA upgrade_distributed_table_before CASCADE;
