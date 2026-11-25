--
-- Shard transfers copy the shards in other transactions, which do not see the
-- uncommitted modifications of the current transaction, so they error out
-- after them.
--
SET citus.shard_replication_factor TO 1;

--
-- Shard moves
--
CREATE SCHEMA move_after_modifications;
SET search_path TO move_after_modifications;
-- some moves below would otherwise wait for their own transaction forever
SET statement_timeout TO '60s';
CREATE TABLE dist (id int PRIMARY KEY, value int);
SELECT create_distributed_table('dist', 'id', shard_count => 8, colocate_with => 'none');
INSERT INTO dist SELECT i, i FROM generate_series(1, 100) i;
SELECT shards[1] AS shard_id, shards[2] AS second_shard_id
FROM (SELECT array_agg(shardid ORDER BY shardid) AS shards
      FROM pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
      WHERE logicalrelid = 'dist'::regclass AND nodeport = :worker_1_port) worker_1_shards \gset
SELECT min(i) AS key_in_shard FROM generate_series(1, 100) i
WHERE get_shard_id_for_distribution_column('dist', i) = :shard_id \gset

-- with block_writes, the write would only commit on the old placement, which
-- is dropped after the move
BEGIN;
UPDATE dist SET value = 0 WHERE id = :key_in_shard;
SELECT citus_move_shard_placement(:shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
ROLLBACK;

-- via logical replication, creating the replication slot would wait for the
-- transaction
BEGIN;
DELETE FROM dist WHERE id = :key_in_shard;
SELECT citus_move_shard_placement(:shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
ROLLBACK;

-- also in a DO block, and after DDL
SELECT set_config('move_after_modifications.shard_id', :'shard_id', false) IS NOT NULL AS set,
       set_config('move_after_modifications.source_port', :'worker_1_port', false) IS NOT NULL AS set,
       set_config('move_after_modifications.target_port', :'worker_2_port', false) IS NOT NULL AS set;
DO $$
BEGIN
    ALTER TABLE move_after_modifications.dist ADD COLUMN extra int;
    PERFORM citus_move_shard_placement(current_setting('move_after_modifications.shard_id')::bigint,
                                       'localhost', current_setting('move_after_modifications.source_port')::int,
                                       'localhost', current_setting('move_after_modifications.target_port')::int,
                                       'block_writes');
END $$;
SELECT nodeport = :worker_1_port AS not_moved FROM pg_dist_shard_placement WHERE shardid = :shard_id;

-- several blocking moves in a transaction do not modify anything in it
BEGIN;
SELECT citus_move_shard_placement(:shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
SELECT citus_move_shard_placement(:second_shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
COMMIT;
SELECT count(*) AS moved FROM pg_dist_shard_placement
WHERE shardid IN (:shard_id, :second_shard_id) AND nodeport = :worker_2_port;
SELECT count(*), sum(value) FROM dist;

-- the executor counts the secondary indexes that a move via logical
-- replication creates on the new placements as modifications, so a blocking
-- move after such a move errors out too, as a split does
CREATE TABLE indexed (id int PRIMARY KEY, value int);
CREATE INDEX ON indexed (value);
SELECT create_distributed_table('indexed', 'id', shard_count => 8, colocate_with => 'none');
SELECT min(shardid) AS indexed_shard_id
FROM pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
WHERE logicalrelid = 'indexed'::regclass AND nodeport = :worker_1_port \gset
SELECT min(shardid) AS third_shard_id
FROM pg_dist_shard_placement JOIN pg_dist_shard USING (shardid)
WHERE logicalrelid = 'dist'::regclass AND nodeport = :worker_1_port \gset
BEGIN;
SELECT citus_move_shard_placement(:indexed_shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');
SELECT citus_move_shard_placement(:third_shard_id, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'block_writes');
ROLLBACK;

RESET statement_timeout;
SELECT public.wait_for_resource_cleanup();
SET client_min_messages TO WARNING;
DROP SCHEMA move_after_modifications CASCADE;
RESET client_min_messages;

--
-- The rebalancer, citus_drain_node() and replicate_table_shards() move or copy
-- the shards in separate transactions, and also error out before they change
-- anything.
--
CREATE SCHEMA rebalance_after_modifications;
SET search_path TO rebalance_after_modifications;
CREATE TABLE dist (id int PRIMARY KEY, value int);
SELECT create_distributed_table('dist', 'id', shard_count => 8, colocate_with => 'none');
INSERT INTO dist SELECT i, i FROM generate_series(1, 100) i;
SELECT shouldhaveshards AS worker_1_should_have_shards FROM pg_dist_node WHERE nodeport = :worker_1_port \gset

BEGIN;
INSERT INTO dist VALUES (101, 101);
SELECT rebalance_table_shards('dist', shard_transfer_mode => 'block_writes');
ROLLBACK;
BEGIN;
INSERT INTO dist VALUES (101, 101);
SELECT citus_drain_node('localhost', :worker_1_port, 'block_writes');
ROLLBACK;
BEGIN;
INSERT INTO dist VALUES (101, 101);
SELECT replicate_table_shards('dist', shard_transfer_mode => 'block_writes');
ROLLBACK;
SELECT shouldhaveshards = :'worker_1_should_have_shards' AS unchanged FROM pg_dist_node WHERE nodeport = :worker_1_port;

SET client_min_messages TO WARNING;
DROP SCHEMA rebalance_after_modifications CASCADE;
RESET client_min_messages;
