CREATE SCHEMA ignoring_orphaned_shards;
SET search_path TO ignoring_orphaned_shards;
-- Use a weird shard count that we don't use in any other tests
SET citus.shard_count TO 13;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 92448000;

CREATE TABLE ref(id int PRIMARY KEY);
SELECT * FROM create_reference_table('ref');

SET citus.next_shard_id TO 92448100;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 92448100;

CREATE TABLE dist1(id int);
SELECT * FROM create_distributed_table('dist1', 'id');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448100 ORDER BY 1;

-- Move first shard, so that the first shard now has 2 placements. One that's
-- active and one that's orphaned.
SELECT citus_move_shard_placement(92448100, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448100 ORDER BY placementid;

-- Add a new table that should get colocated with dist1 automatically, but
-- should not get a shard for the orphaned placement.
SET citus.next_shard_id TO 92448200;
CREATE TABLE dist2(id int);
SELECT * FROM create_distributed_table('dist2', 'id');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448100 ORDER BY 1;
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448200 ORDER BY placementid;

-- uncolocate it
SELECT update_distributed_table_colocation('dist2', 'none');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448100 ORDER BY 1;
-- Make sure we can add it back to the colocation, even though it has a
-- different number of shard placements for the first shard.
SELECT update_distributed_table_colocation('dist2', 'dist1');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448100 ORDER BY 1;

-- Make sure that replication count check in FOR UPDATE ignores orphaned
-- shards.
SELECT * FROM dist1 WHERE id = 1 FOR UPDATE;
-- Make sure we don't send a query to the orphaned shard
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
INSERT INTO dist1 VALUES (1);
ROLLBACK;

-- Make sure we can create a foreign key on community edition, because
-- replication factor is 1
ALTER TABLE dist1
ADD CONSTRAINT dist1_ref_fk
FOREIGN KEY (id)
REFERENCES ref(id);

SET citus.shard_replication_factor TO 2;
SET citus.next_shard_id TO 92448300;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 92448300;
CREATE TABLE rep1(id int);
SELECT * FROM create_distributed_table('rep1', 'id');

-- Add the coordinator, so we can have a replicated shard
SELECT 1 FROM citus_add_node('localhost', :master_port, 0);
SELECT 1 FROM citus_set_node_property('localhost', :master_port, 'shouldhaveshards', true);
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448300 ORDER BY 1;

SELECT citus_move_shard_placement(92448300, 'localhost', :worker_1_port, 'localhost', :master_port);
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448300 ORDER BY placementid;

-- Add a new table that should get colocated with rep1 automatically, but
-- should not get a shard for the orphaned placement.
SET citus.next_shard_id TO 92448400;
CREATE TABLE rep2(id int);
SELECT * FROM create_distributed_table('rep2', 'id');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448300 ORDER BY 1;
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448400 ORDER BY placementid;

-- uncolocate it
SELECT update_distributed_table_colocation('rep2', 'none');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448300 ORDER BY 1;
-- Make sure we can add it back to the colocation, even though it has a
-- different number of shard placements for the first shard.
SELECT update_distributed_table_colocation('rep2', 'rep1');
SELECT logicalrelid FROM pg_dist_partition WHERE colocationid = 92448300 ORDER BY 1;

UPDATE pg_dist_placement SET shardstate = 3 WHERE shardid = 92448300 AND groupid = 0;
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448300 ORDER BY placementid;

-- cannot copy from an orphaned shard
SELECT * FROM citus_copy_shard_placement(92448300, 'localhost', :worker_1_port, 'localhost', :master_port);
-- cannot copy to an orphaned shard
SELECT * FROM citus_copy_shard_placement(92448300, 'localhost', :worker_2_port, 'localhost', :worker_1_port);
-- can still copy to an inactive shard
SELECT * FROM citus_copy_shard_placement(92448300, 'localhost', :worker_2_port, 'localhost', :master_port);
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448300 ORDER BY placementid;

-- Make sure we don't send a query to the orphaned shard
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
SET LOCAL citus.log_local_commands TO ON;
INSERT INTO rep1 VALUES (1);
ROLLBACK;

-- Cause the orphaned shard to be local
SELECT 1 FROM citus_drain_node('localhost', :master_port);
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448300 ORDER BY placementid;

-- Make sure we don't send a query to the orphaned shard if it's local
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
SET LOCAL citus.log_local_commands TO ON;
INSERT INTO rep1 VALUES (1);
ROLLBACK;


SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 92448500;
CREATE TABLE range1(id int);
SELECT create_distributed_table('range1', 'id', 'range');
CALL public.create_range_partitioned_shards('range1', '{0,3}','{2,5}');

-- Move shard placement and clean it up
SELECT citus_move_shard_placement(92448500, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes');
CALL citus_cleanup_orphaned_shards();
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448300 ORDER BY placementid;

SET citus.next_shard_id TO 92448600;
CREATE TABLE range2(id int);
SELECT create_distributed_table('range2', 'id', 'range');
CALL public.create_range_partitioned_shards('range2', '{0,3}','{2,5}');

-- Move shard placement and DON'T clean it up, now range1 and range2 are
-- colocated, but only range2 has an orphaned shard.
SELECT citus_move_shard_placement(92448600, 'localhost', :worker_2_port, 'localhost', :worker_1_port, 'block_writes');
SELECT shardid, shardstate, nodeport FROM pg_dist_shard_placement WHERE shardid = 92448600 ORDER BY placementid;

-- Make sure that tables are detected as colocated
SELECT * FROM range1 JOIN range2 ON range1.id = range2.id;

-- Make sure we can create a foreign key on community edition, because
-- replication factor is 1
ALTER TABLE range1
ADD CONSTRAINT range1_ref_fk
FOREIGN KEY (id)
REFERENCES ref(id);

SET client_min_messages TO WARNING;
DROP SCHEMA ignoring_orphaned_shards CASCADE;
