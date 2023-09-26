SET search_path TO upgrade_basic, public, pg_catalog;
BEGIN;
-- We have the tablename filter to avoid adding an alternative output for when the coordinator is in metadata vs when not
SELECT * FROM pg_indexes WHERE schemaname = 'upgrade_basic' and tablename NOT LIKE 'r_%' ORDER BY tablename;

SELECT logicalrelid FROM pg_dist_partition
  JOIN pg_depend ON logicalrelid=objid
  JOIN pg_catalog.pg_class ON logicalrelid=oid
  WHERE
   refobjid=(select oid FROM pg_extension WHERE extname = 'citus')
   AND relnamespace='upgrade_basic'::regnamespace
  ORDER BY logicalrelid;

SELECT tgrelid::regclass, tgfoid::regproc, tgisinternal, tgenabled, tgtype::int4::bit(8)
  FROM pg_dist_partition
  JOIN pg_trigger ON tgrelid=logicalrelid
  JOIN pg_class ON pg_class.oid=logicalrelid
  WHERE
    relnamespace='upgrade_basic'::regnamespace
    AND tgname LIKE 'truncate_trigger_%'
  ORDER BY tgrelid::regclass;


SELECT * FROM t ORDER BY a;
SELECT * FROM t WHERE a = 1;

INSERT INTO t SELECT * FROM generate_series(10, 15);

EXPLAIN (COSTS FALSE) SELECT * from t;
EXPLAIN (COSTS FALSE) SELECT * from t WHERE a = 1;

SELECT * FROM t WHERE a = 10;
SELECT * FROM t WHERE a = 11;

COPY t FROM PROGRAM 'echo 20 && echo 21 && echo 22 && echo 23 && echo 24' WITH CSV;

ALTER TABLE t ADD COLUMN b int DEFAULT 10;
SELECT * FROM t ORDER BY a;

TRUNCATE TABLE t;

SELECT * FROM T;
DROP TABLE t;
\d t


-- verify that the table whose column is dropped before a pg_upgrade still works as expected.
SELECT * FROM t_ab ORDER BY b;
SELECT * FROM t_ab WHERE b = 11;
SELECT * FROM t_ab WHERE b = 22;


-- Check that we can create a distributed table out of a table that was created
-- before the upgrade
SELECT * FROM t2 ORDER BY a;
SELECT create_distributed_table('t2', 'a');
SELECT * FROM t2 ORDER BY a;
ROLLBACK;

BEGIN;
SET LOCAL citus.multi_shard_modify_mode TO 'sequential';

SELECT * FROM r ORDER BY a;
SELECT * FROM tr ORDER BY pk;
DELETE FROM r where a = 1;
SELECT * FROM r ORDER BY a;
SELECT * FROM tr ORDER BY pk;

UPDATE r SET a = 30 WHERE a = 3;
SELECT * FROM r ORDER BY a;
SELECT * FROM tr ORDER BY pk;

-- Check we can still create distributed tables after upgrade
CREATE TABLE t3(a int, b int);
SELECT create_distributed_table('t3', 'a');
INSERT INTO t3 VALUES (1, 11);
INSERT INTO t3 VALUES (2, 22);
INSERT INTO t3 VALUES (3, 33);

SELECT * FROM t3 ORDER BY a;

SELECT shardminvalue, shardmaxvalue FROM pg_dist_shard
  WHERE logicalrelid = 't_range'::regclass
  ORDER BY shardminvalue, shardmaxvalue;

SELECT * FROM t_range ORDER BY id;

SELECT master_create_empty_shard('t_range')  AS new_shard_id \gset
UPDATE pg_dist_shard SET shardminvalue = '9', shardmaxvalue = '11' WHERE shardid = :new_shard_id;
\copy t_range FROM STDIN with (DELIMITER ',')
9,2
10,3
11,4
\.

SELECT shardminvalue, shardmaxvalue FROM pg_dist_shard
  WHERE logicalrelid = 't_range'::regclass
  ORDER BY shardminvalue, shardmaxvalue;

SELECT * FROM t_range ORDER BY id;


ROLLBACK;

-- There is a difference in partkey Var representation between PG16 and older versions
-- Sanity check here that we can properly do column_to_column_name
SELECT column_to_column_name(logicalrelid, partkey)
FROM pg_dist_partition WHERE partkey IS NOT NULL ORDER BY 1 LIMIT 1;
