SET search_path TO upgrade_basic, public, pg_catalog;
BEGIN;

SELECT * FROM pg_indexes WHERE schemaname = 'upgrade_basic' ORDER BY tablename;

SELECT nextval('pg_dist_shardid_seq') = MAX(shardid)+1 FROM pg_dist_shard;
SELECT nextval('pg_dist_placement_placementid_seq') = MAX(placementid)+1 FROM pg_dist_placement;
SELECT nextval('pg_dist_groupid_seq') = MAX(groupid)+1 FROM pg_dist_node;
SELECT nextval('pg_dist_node_nodeid_seq') = MAX(nodeid)+1 FROM pg_dist_node;
SELECT nextval('pg_dist_colocationid_seq') = MAX(colocationid)+1 FROM pg_dist_colocation;

-- If this query gives output it means we've added a new sequence that should
-- possibly be restored after upgrades.
SELECT sequence_name FROM information_schema.sequences
  WHERE sequence_name LIKE 'pg_dist_%'
  AND sequence_name NOT IN (
    -- these ones are restored above
    'pg_dist_shardid_seq',
    'pg_dist_placement_placementid_seq',
    'pg_dist_groupid_seq',
    'pg_dist_node_nodeid_seq',
    'pg_dist_colocationid_seq'
  );

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
  WHERE logicalrelid = 't_append'::regclass
  ORDER BY shardminvalue, shardmaxvalue;

SELECT * FROM t_append ORDER BY id;

\copy t_append FROM STDIN DELIMITER ','
9,2
10,3
11,4
\.

SELECT shardminvalue, shardmaxvalue FROM pg_dist_shard
  WHERE logicalrelid = 't_append'::regclass
  ORDER BY shardminvalue, shardmaxvalue;

SELECT * FROM t_append ORDER BY id;


ROLLBACK;
