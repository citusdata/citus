CREATE SCHEMA local_shard_execution_dropped_column;
SET search_path TO local_shard_execution_dropped_column;

SET citus.next_shard_id TO 2460000;

-- the scenario is described on https://github.com/citusdata/citus/issues/5038

-- first stop the metadata syncing to the node do that drop column
-- is not propogated
SELECT stop_metadata_sync_to_node('localhost',:worker_1_port);
SELECT stop_metadata_sync_to_node('localhost',:worker_2_port);

-- create a distributed table, drop a column and sync the metadata
SET citus.shard_replication_factor TO 1;
CREATE TABLE t1 (a int, b int, c int UNIQUE);
SELECT create_distributed_table('t1', 'c');
ALTER TABLE t1 DROP COLUMN b;

SELECT 1 FROM citus_activate_node('localhost',:worker_1_port);
SELECT 1 FROM citus_activate_node('localhost',:worker_2_port);

\c - - - :worker_1_port
SET search_path TO local_shard_execution_dropped_column;

-- show the dropped columns
SELECT attrelid::regclass, attname, attnum, attisdropped
FROM pg_attribute WHERE attrelid IN ('t1'::regclass, 't1_2460000'::regclass) and attname NOT IN ('tableoid','cmax', 'xmax', 'cmin', 'xmin', 'ctid')
ORDER BY 1, 3, 2, 4;

-- connect to a worker node where local execution is done
prepare p1(int) as insert into t1(a,c) VALUES (5,$1) ON CONFLICT (c) DO NOTHING;
SET citus.log_remote_commands TO ON;
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);

prepare p2(int) as SELECT count(*) FROM t1 WHERE c = $1 GROUP BY c;
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);

prepare p3(int) as INSERT INTO t1(a,c) VALUES (5, $1), (6, $1), (7, $1),(5, $1), (6, $1), (7, $1) ON CONFLICT DO NOTHING;
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);

prepare p4(int) as UPDATE t1 SET a = a + 1 WHERE c = $1;
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);
execute p4(8);

\c - - - :master_port

-- one another combination is that the shell table
-- has a dropped column but not the shard, via rebalance operation
SET search_path TO local_shard_execution_dropped_column;
ALTER TABLE t1 DROP COLUMN a;

SELECT citus_move_shard_placement(2460000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');

\c - - - :worker_2_port
SET search_path TO local_shard_execution_dropped_column;

-- show the dropped columns
SELECT attrelid::regclass, attname, attnum, attisdropped
FROM pg_attribute WHERE attrelid IN ('t1'::regclass, 't1_2460000'::regclass) and attname NOT IN ('tableoid','cmax', 'xmax', 'cmin', 'xmin', 'ctid')
ORDER BY 1, 3, 2, 4;

prepare p1(int) as insert into t1(c) VALUES ($1) ON CONFLICT (c) DO NOTHING;
SET citus.log_remote_commands TO ON;
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);
execute p1(8);

prepare p2(int) as SELECT count(*) FROM t1 WHERE c = $1 GROUP BY c;
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);
execute p2(8);

prepare p3(int) as INSERT INTO t1(c) VALUES ($1),($1),($1),($1),($1),($1),($1),($1),($1),($1),($1),($1),($1),($1) ON CONFLICT DO NOTHING;
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);
execute p3(8);

\c - - - :master_port
DROP SCHEMA local_shard_execution_dropped_column CASCADE;
