ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 820000;
SELECT groupid AS worker_2_group FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
SELECT groupid AS worker_1_group FROM pg_dist_node WHERE nodeport=:worker_1_port \gset

-- ===================================================================
-- test shard repair functionality
-- ===================================================================

-- create a table and create its distribution metadata
CREATE TABLE customer_engagements ( id integer, created_at date, event_data text );

-- add some indexes
CREATE INDEX ON customer_engagements (id);
CREATE INDEX ON customer_engagements (created_at);
CREATE INDEX ON customer_engagements (event_data);

-- distribute the table
SELECT master_create_distributed_table('customer_engagements', 'id', 'hash');

-- create a single shard on the first worker
SELECT master_create_worker_shards('customer_engagements', 1, 2);

-- ingest some data for the tests
INSERT INTO customer_engagements VALUES (1, '01-01-2015', 'first event');
INSERT INTO customer_engagements VALUES (2, '02-01-2015', 'second event');
INSERT INTO customer_engagements VALUES (1, '03-01-2015', 'third event');

-- the following queries does the following:
-- (i)    create a new shard
-- (ii)   mark the second shard placements as unhealthy
-- (iii)  do basic checks i.e., only allow copy from healthy placement to unhealthy ones 
-- (iv)   do a successful master_copy_shard_placement from the first placement to the second
-- (v)    mark the first placement as unhealthy and execute a query that is routed to the second placement

-- get the newshardid
SELECT shardid as newshardid FROM pg_dist_shard WHERE logicalrelid = 'customer_engagements'::regclass
\gset

-- now, update the second placement as unhealthy
UPDATE pg_dist_placement SET shardstate = 3 WHERE shardid = :newshardid
  AND groupid = :worker_2_group;

-- cannot repair a shard after a modification (transaction still open during repair)
BEGIN;
ALTER TABLE customer_engagements ADD COLUMN value float;
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
ROLLBACK;

BEGIN;
INSERT INTO customer_engagements VALUES (4, '04-01-2015', 'fourth event');
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
ROLLBACK;

-- modifications after reparing a shard are fine (will use new metadata)
BEGIN;
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
ALTER TABLE customer_engagements ADD COLUMN value float;
ROLLBACK;

BEGIN;
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
INSERT INTO customer_engagements VALUES (4, '04-01-2015', 'fourth event');
ROLLBACK;

-- add a fake healthy placement for the tests

INSERT INTO pg_dist_placement (groupid, shardid, shardstate, shardlength)
							 VALUES (:worker_2_group, :newshardid, 1, 0);

SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

DELETE FROM pg_dist_placement
  WHERE groupid = :worker_2_group AND shardid = :newshardid AND shardstate = 1;

-- also try to copy from an inactive placement
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_2_port, 'localhost', :worker_1_port);

-- "copy" this shard from the first placement to the second one
SELECT master_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);

-- now, update first placement as unhealthy (and raise a notice) so that queries are not routed to there
UPDATE pg_dist_placement SET shardstate = 3 WHERE shardid = :newshardid AND groupid = :worker_1_group;

-- get the data from the second placement
SELECT * FROM customer_engagements;

-- now do the same test over again with a foreign table
CREATE FOREIGN TABLE remote_engagements (
	id integer,
	created_at date,
	event_data text
) SERVER fake_fdw_server;

-- distribute the table
SELECT master_create_distributed_table('remote_engagements', 'id', 'hash');

-- create a single shard on the first worker
SELECT master_create_worker_shards('remote_engagements', 1, 2);

-- get the newshardid
SELECT shardid as remotenewshardid FROM pg_dist_shard WHERE logicalrelid = 'remote_engagements'::regclass
\gset

-- now, update the second placement as unhealthy
UPDATE pg_dist_placement SET shardstate = 3 WHERE shardid = :remotenewshardid AND groupid = :worker_2_group;

-- oops! we don't support repairing shards backed by foreign tables
SELECT master_copy_shard_placement(:remotenewshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port);
