--
-- MULTI_COLOCATED_SHARD_REBALANCE
--

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 13000000;

SET citus.shard_count TO 6;
SET citus.shard_replication_factor TO 1;

-- create distributed tables
CREATE TABLE table1_group1 ( id int PRIMARY KEY);
SELECT create_distributed_table('table1_group1', 'id', 'hash');

CREATE TABLE table2_group1 ( id int );
SELECT create_distributed_table('table2_group1', 'id', 'hash');

SET citus.shard_count TO 8;
CREATE TABLE table5_groupX ( id int );
SELECT create_distributed_table('table5_groupX', 'id', 'hash');

CREATE TABLE table6_append ( id int );
SELECT create_distributed_table('table6_append', 'id', 'append');
SELECT master_create_empty_shard('table6_append');
SELECT master_create_empty_shard('table6_append');

-- Mark tables as non-mx tables, in order to be able to test citus_copy_shard_placement
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid IN
    ('table1_group1'::regclass, 'table2_group1'::regclass, 'table5_groupX'::regclass);

-- test copy

-- test copying colocated shards
-- status before shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;

-- try to copy colocated shards without a replica identity
SELECT citus_copy_shard_placement(13000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- copy colocated shards
SELECT citus_copy_shard_placement(13000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

-- error out if trying to move a shard that already has a placement on the target
SELECT citus_move_shard_placement(13000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

-- status after shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;

-- also connect worker to verify we successfully copied given shard (and other colocated shards)
\c - - - :worker_2_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table1_group1_13000000'::regclass;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table2_group1_13000006'::regclass;
\c - - - :master_port


-- copy colocated shards again to see warning
SELECT citus_copy_shard_placement(13000000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');


-- test copying NOT colocated shard
-- status before shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass
ORDER BY s.shardid, sp.nodeport;

-- copy NOT colocated shard
SELECT citus_copy_shard_placement(13000012, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

-- status after shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass
ORDER BY s.shardid, sp.nodeport;


-- test copying shard in append distributed table
-- status before shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass
ORDER BY s.shardid, sp.nodeport;

-- copy shard in append distributed table
SELECT citus_copy_shard_placement(13000020, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');

-- status after shard copy
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass
ORDER BY s.shardid, sp.nodeport;


-- test move

-- test moving colocated shards
-- status before shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
ORDER BY s.shardid, sp.nodeport;

-- move colocated shards
SELECT master_move_shard_placement(13000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');

-- status after shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
    AND sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;

-- moving the shard again is idempotent
SELECT citus_move_shard_placement(13000001, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');

-- also connect worker to verify we successfully moved given shard (and other colocated shards)
\c - - - :worker_1_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table1_group1_13000001'::regclass;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table2_group1_13000007'::regclass;
\c - - - :master_port



-- test moving NOT colocated shard
-- status before shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass
    AND sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;

-- move NOT colocated shard
SELECT master_move_shard_placement(13000013, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');

-- status after shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table5_groupX'::regclass AND
    sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;


-- test moving shard in append distributed table
-- status before shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass
    AND sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;

-- move shard in append distributed table
SELECT master_move_shard_placement(13000021, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

-- status after shard move
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
    p.logicalrelid = 'table6_append'::regclass AND
    sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;


-- try to move shard from wrong node
SELECT master_move_shard_placement(13000021, 'localhost', :master_port, 'localhost', :worker_1_port, 'force_logical');


-- test shard move with foreign constraints
DROP TABLE IF EXISTS table1_group1, table2_group1;

SET citus.shard_count TO 6;
SET citus.shard_replication_factor TO 1;

-- create distributed tables
CREATE TABLE table1_group1 ( id int PRIMARY KEY);
SELECT create_distributed_table('table1_group1', 'id', 'hash');

CREATE TABLE table2_group1 ( id int, table1_id int, FOREIGN KEY(table1_id) REFERENCES table1_group1(id));
SELECT create_distributed_table('table2_group1', 'table1_id', 'hash');

-- Mark the tables as non-mx tables
UPDATE pg_dist_partition SET repmodel='c' WHERE logicalrelid IN
    ('table1_group1'::regclass, 'table2_group1'::regclass);

-- status before shard rebalance
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
	colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
    AND sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;

SELECT master_move_shard_placement(13000022, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');

-- status after shard rebalance
SELECT s.shardid, s.logicalrelid::regclass, sp.nodeport
FROM
    pg_dist_partition p, pg_dist_shard s, pg_dist_shard_placement sp
WHERE
    p.logicalrelid = s.logicalrelid AND
    s.shardid = sp.shardid AND
	colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'table1_group1'::regclass)
    AND sp.shardstate != 4
ORDER BY s.shardid, sp.nodeport;

-- also connect worker to verify we successfully moved given shard (and other colocated shards)
\c - - - :worker_2_port
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table1_group1_13000022'::regclass;
SELECT "Column", "Type", "Modifiers" FROM table_desc WHERE relid='public.table2_group1_13000028'::regclass;

-- make sure that we've created the foreign keys
SELECT  "Constraint", "Definition" FROM table_fkeys
  WHERE "Constraint" LIKE 'table2_group%' OR "Constraint" LIKE 'table1_group%';

\c - - - :master_port



-- test shard copy with foreign constraints
-- we expect it to error out because we do not support foreign constraints with replication factor > 1
SELECT citus_copy_shard_placement(13000022, 'localhost', :worker_2_port, 'localhost', :worker_1_port);


-- lets also test that master_move_shard_placement doesn't break serials
CREATE TABLE serial_move_test (key int, other_val serial);
SET citus.shard_replication_factor TO 1;

SELECT create_distributed_table('serial_move_test', 'key');

-- key 15 goes to shard 13000035
INSERT INTO serial_move_test (key) VALUES (15) RETURNING *;
INSERT INTO serial_move_test (key) VALUES (15) RETURNING *;

-- confirm the shard id
SELECT * FROM run_command_on_placements('serial_move_test', 'SELECT DISTINCT key FROM %s WHERE key = 15') WHERE result = '15' AND shardid = 13000034;

SELECT master_move_shard_placement(13000034, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
SELECT public.wait_for_resource_cleanup();

-- confirm the successfull move
SELECT * FROM run_command_on_placements('serial_move_test', 'SELECT DISTINCT key FROM %s WHERE key = 15') WHERE result = '15' AND shardid = 13000034;

-- finally show that serials work fine afterwards
INSERT INTO serial_move_test (key) VALUES (15) RETURNING *;
INSERT INTO serial_move_test (key) VALUES (15) RETURNING *;


-- lets do some failure testing
CREATE TABLE logical_failure_test (key int);
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SELECT create_distributed_table('logical_failure_test', 'key');

-- ensure that the shard is created for this user
\c - - - :worker_2_port
\dt logical_failure_test_13000038

DROP TABLE logical_failure_test_13000038;

-- should fail since the command wouldn't be able to connect to the worker_1
\c - - - :master_port
SELECT master_move_shard_placement(13000038, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical');
SELECT public.wait_for_resource_cleanup();

DROP TABLE logical_failure_test;

-- lets test the logical replication modes
CREATE TABLE test_with_pkey (key int PRIMARY KEY, value int NOT NULL);
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SELECT create_distributed_table('test_with_pkey', 'key', colocate_with => 'none');

-- should succeed since there is a replica identity defined
SELECT master_move_shard_placement(13000042, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT public.wait_for_resource_cleanup();

-- should succeed since we still have a replica identity
ALTER TABLE test_with_pkey REPLICA IDENTITY FULL;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'auto');
SELECT public.wait_for_resource_cleanup();

-- make sure we have the replica identity after the move
SELECT result FROM run_command_on_placements( 'test_with_pkey', 'SELECT relreplident FROM pg_class WHERE relname = ''%s''') WHERE shardid = 13000042;

-- this time should fail since we don't have replica identity any more
ALTER TABLE test_with_pkey REPLICA IDENTITY NOTHING;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'auto');
SELECT public.wait_for_resource_cleanup();

-- make sure we have the replica identity after the move
SELECT result FROM run_command_on_placements( 'test_with_pkey', 'SELECT relreplident FROM pg_class WHERE relname = ''%s''') WHERE shardid = 13000042;

-- should succeed since we still have a replica identity
ALTER TABLE test_with_pkey REPLICA IDENTITY USING INDEX test_with_pkey_pkey;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
SELECT public.wait_for_resource_cleanup();

-- make sure we have the replica identity after the move
SELECT result FROM run_command_on_placements( 'test_with_pkey', 'SELECT relreplident FROM pg_class WHERE relname = ''%s''') WHERE shardid = 13000042;

-- one final test with shard_transfer_mode auto
CREATE UNIQUE INDEX req_rep_idx ON test_with_pkey(key, value);
ALTER TABLE test_with_pkey REPLICA IDENTITY USING INDEX req_rep_idx;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'auto');
SELECT public.wait_for_resource_cleanup();

-- make sure we have the replica identity after the move
SELECT result FROM run_command_on_placements( 'test_with_pkey', 'SELECT relreplident FROM pg_class WHERE relname = ''%s''') WHERE shardid = 13000042;

ALTER TABLE test_with_pkey REPLICA IDENTITY NOTHING;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
SELECT public.wait_for_resource_cleanup();

-- make sure we have the replica identity after the move
SELECT result FROM run_command_on_placements( 'test_with_pkey', 'SELECT relreplident FROM pg_class WHERE relname = ''%s''') WHERE shardid = 13000042;

-- should succeed but not use logical replication
ALTER TABLE test_with_pkey REPLICA IDENTITY NOTHING;

SET client_min_messages TO DEBUG1;
SELECT master_move_shard_placement(13000042, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes');

SET client_min_messages TO DEFAULT;
SELECT public.wait_for_resource_cleanup();

-- we don't support multiple shard moves in a single transaction
SELECT
    master_move_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical')
FROM
    pg_dist_shard_placement where nodeport = :worker_1_port AND
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_with_pkey'::regclass);
SELECT public.wait_for_resource_cleanup();

-- similar test with explicit transaction block
BEGIN;

    SELECT master_move_shard_placement(13000042, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
    SELECT master_move_shard_placement(13000044, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
COMMIT;
    SELECT public.wait_for_resource_cleanup();

-- we do support the same with block writes
SELECT
    master_move_shard_placement(shardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='block_writes')
FROM
    pg_dist_shard_placement where nodeport = :worker_1_port AND
    shardid IN (SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'test_with_pkey'::regclass);
SELECT public.wait_for_resource_cleanup();

-- we should be able to move shard placements after COMMIT/ABORT
BEGIN;

    SELECT master_move_shard_placement(13000043, 'localhost', :worker_2_port, 'localhost', :worker_1_port, shard_transfer_mode:='force_logical');
COMMIT;
SELECT public.wait_for_resource_cleanup();

SELECT master_move_shard_placement(13000045, 'localhost', :worker_2_port, 'localhost', :worker_1_port, shard_transfer_mode:='force_logical');
SELECT public.wait_for_resource_cleanup();

BEGIN;

    SELECT master_move_shard_placement(13000043, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
ABORT;

SELECT master_move_shard_placement(13000045, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
SELECT public.wait_for_resource_cleanup();

-- we should be able to move shard placements of partitioend tables
CREATE SCHEMA move_partitions;
CREATE TABLE move_partitions.events (
    id serial,
    t timestamptz default now(),
    payload text
)
PARTITION BY RANGE(t);

SET citus.shard_count TO 6;
SELECT create_distributed_table('move_partitions.events', 'id', colocate_with := 'none');

CREATE TABLE move_partitions.events_1 PARTITION OF move_partitions.events
FOR VALUES FROM ('2015-01-01') TO ('2016-01-01');

INSERT INTO move_partitions.events (t, payload)
SELECT '2015-01-01'::date + (interval '1 day' * s), s FROM generate_series(1, 100) s;

SELECT count(*) FROM move_partitions.events;

-- try to move automatically
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port)
FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'move_partitions.events'::regclass AND nodeport = :worker_2_port
AND shardstate != 4
ORDER BY shardid LIMIT 1;
SELECT public.wait_for_resource_cleanup();

-- force logical replication
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'force_logical')
FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'move_partitions.events'::regclass AND nodeport = :worker_2_port
ORDER BY shardid LIMIT 1;
SELECT public.wait_for_resource_cleanup();

SELECT count(*) FROM move_partitions.events;

-- add a primary key to the partition
ALTER TABLE move_partitions.events_1 ADD CONSTRAINT e_1_pk PRIMARY KEY (id);

-- should be able to move automatically now
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port)
FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'move_partitions.events'::regclass AND nodeport = :worker_2_port AND shardstate != 4
ORDER BY shardid LIMIT 1;
SELECT public.wait_for_resource_cleanup();

SELECT count(*) FROM move_partitions.events;

-- should also be able to move with block writes
SELECT master_move_shard_placement(shardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes')
FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'move_partitions.events'::regclass AND nodeport = :worker_2_port AND shardstate != 4
ORDER BY shardid LIMIT 1;
SELECT public.wait_for_resource_cleanup();

SELECT count(*) FROM move_partitions.events;

-- should have moved all shards to node 1 (2*6 = 12)
SELECT count(*)
FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid::text LIKE 'move_partitions.events%' AND nodeport = :worker_1_port;

DROP TABLE move_partitions.events;

-- set back to the defaults and drop the tables
SET client_min_messages TO DEFAULT;
DROP TABLE test_with_pkey;
DROP TABLE table2_group1;
DROP TABLE table1_group1;
DROP TABLE table5_groupX;
DROP TABLE table6_append;
DROP TABLE serial_move_test;
DROP SCHEMA move_partitions CASCADE;
