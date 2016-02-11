-- ===================================================================
-- create test functions
-- ===================================================================

CREATE FUNCTION load_shard_id_array(regclass)
	RETURNS bigint[]
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_interval_array(bigint, anyelement)
	RETURNS anyarray
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION load_shard_placement_array(bigint, bool)
	RETURNS text[]
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_column_id(regclass)
	RETURNS smallint
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION partition_type(regclass)
	RETURNS "char"
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION is_distributed_table(regclass)
	RETURNS boolean
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION column_name_to_column_id(regclass, cstring)
	RETURNS smallint
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION create_monolithic_shard_row(regclass)
	RETURNS bigint
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION create_healthy_local_shard_placement_row(bigint)
	RETURNS void
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION delete_shard_placement_row(bigint, text, bigint)
	RETURNS bool
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION update_shard_placement_row_state(bigint, text, bigint, int)
	RETURNS bool
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION acquire_shared_shard_lock(bigint)
	RETURNS void
	AS 'citusdb'
	LANGUAGE C STRICT;

CREATE FUNCTION column_name_to_column(regclass, text)
	RETURNS text
	AS 'citusdb'
	LANGUAGE C STRICT;

-- ===================================================================
-- test distribution metadata functionality
-- ===================================================================

-- create table to be distributed
CREATE TABLE events_hash (
	id bigint,
	name text
);

-- for this table we'll "distribute" manually but verify using function calls
INSERT INTO pg_dist_shard
	(shardid, logicalrelid, shardstorage, shardminvalue, shardmaxvalue)
VALUES
	(1, 'events_hash'::regclass, 't', '0', '10'),
	(2, 'events_hash'::regclass, 't', '10', '20'),
	(3, 'events_hash'::regclass, 't', '20', '30'),
	(4, 'events_hash'::regclass, 't', '30', '40');

INSERT INTO pg_dist_shard_placement
	(nodename, nodeport, shardid, shardstate, shardlength)
VALUES
	('cluster-worker-01', 5432, 1, 0, 0),
	('cluster-worker-01', 5432, 2, 0, 0),
	('cluster-worker-02', 5433, 3, 0, 0),
	('cluster-worker-02', 5433, 4, 0, 0),
	('cluster-worker-03', 5434, 1, 1, 0),
	('cluster-worker-03', 5434, 2, 1, 0),
	('cluster-worker-04', 5435, 3, 1, 0),
	('cluster-worker-04', 5435, 4, 1, 0);

INSERT INTO pg_dist_partition (logicalrelid, partmethod, partkey)
VALUES
	('events_hash'::regclass, 'h', column_name_to_column('events_hash'::regclass, 'name'));

-- should see above shard identifiers
SELECT load_shard_id_array('events_hash');

-- should see array with first shard range
SELECT load_shard_interval_array(1, 0);

-- should even work for range-partitioned shards
BEGIN;
	UPDATE pg_dist_shard SET
		shardminvalue = 'Aardvark',
		shardmaxvalue = 'Zebra'
	WHERE shardid = 1;

	UPDATE pg_dist_partition SET partmethod = 'r'
	WHERE logicalrelid = 'events_hash'::regclass;

	SELECT load_shard_interval_array(1, ''::text);
ROLLBACK;

-- should see error for non-existent shard
SELECT load_shard_interval_array(5, 0);

-- should see two placements
SELECT load_shard_placement_array(2, false);

-- only one of which is finalized
SELECT load_shard_placement_array(2, true);

-- should see error for non-existent shard
SELECT load_shard_placement_array(6, false);

-- should see column id of 'name'
SELECT partition_column_id('events_hash');

-- should see hash partition type and fail for non-distributed tables
SELECT partition_type('events_hash');
SELECT partition_type('pg_type');

-- should see true for events_hash, false for others
SELECT is_distributed_table('events_hash');
SELECT is_distributed_table('pg_type');
SELECT is_distributed_table('pg_dist_shard');

-- test underlying column name-id translation
SELECT column_name_to_column_id('events_hash', 'name');
SELECT column_name_to_column_id('events_hash', 'ctid');
SELECT column_name_to_column_id('events_hash', 'non_existent');

-- drop shard rows (must drop placements first)
DELETE FROM pg_dist_shard_placement
	WHERE shardid BETWEEN 1 AND 4;
DELETE FROM pg_dist_shard
	WHERE logicalrelid = 'events_hash'::regclass;

-- verify that an eager load shows them missing
SELECT load_shard_id_array('events_hash');

-- create second table to distribute
CREATE TABLE customers (
	id bigint,
	name text
);

-- now we'll distribute using function calls but verify metadata manually...

-- partition on id and manually inspect partition row
INSERT INTO pg_dist_partition (logicalrelid, partmethod, partkey)
VALUES
	('customers'::regclass, 'h', column_name_to_column('customers'::regclass, 'id'));
SELECT partmethod, partkey FROM pg_dist_partition
	WHERE logicalrelid = 'customers'::regclass;

-- make one huge shard and manually inspect shard row
SELECT create_monolithic_shard_row('customers') AS new_shard_id
\gset
SELECT shardstorage, shardminvalue, shardmaxvalue FROM pg_dist_shard
WHERE shardid = :new_shard_id;

-- add a placement and manually inspect row
SELECT create_healthy_local_shard_placement_row(:new_shard_id);
SELECT shardstate, nodename, nodeport FROM pg_dist_shard_placement
WHERE shardid = :new_shard_id AND nodename = 'localhost' and nodeport = 5432;

-- mark it as unhealthy and inspect
SELECT update_shard_placement_row_state(:new_shard_id, 'localhost', 5432, 3);
SELECT shardstate FROM pg_dist_shard_placement
WHERE shardid = :new_shard_id AND nodename = 'localhost' and nodeport = 5432;

-- remove it and verify it is gone
SELECT delete_shard_placement_row(:new_shard_id, 'localhost', 5432);
SELECT COUNT(*) FROM pg_dist_shard_placement
WHERE shardid = :new_shard_id AND nodename = 'localhost' and nodeport = 5432;

-- deleting or updating a non-existent row should fail
SELECT delete_shard_placement_row(:new_shard_id, 'wrong_localhost', 5432);
SELECT update_shard_placement_row_state(:new_shard_id, 'localhost', 5432, 3);

-- now we'll even test our lock methods...

-- use transaction to bound how long we hold the lock
BEGIN;

-- pick up a shard lock and look for it in pg_locks
SELECT acquire_shared_shard_lock(5);
SELECT objid, mode FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;

-- commit should drop the lock
COMMIT;

-- lock should be gone now
SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory' AND objid = 5;
