--
-- MULTI_PARTITION_PRUNING
--

-- Tests to verify that we correctly prune unreferenced shards. For this, we
-- need to increase the logging verbosity of messages displayed on the client.


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 770000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 770000;


SET citus.explain_distributed_queries TO off;
SET client_min_messages TO DEBUG2;

-- Adding additional l_orderkey = 1 to make this query not router executable
SELECT l_orderkey, l_linenumber, l_shipdate FROM lineitem WHERE l_orderkey = 9030 or l_orderkey = 1;

-- We use the l_linenumber field for the following aggregations. We need to use
-- an integer type, as aggregations on numerics or big integers return numerics
-- of unknown length. When the numerics are read into our temporary table, they
-- trigger the the creation of toasted tables and indexes. This in turn prints
-- non-deterministic debug messages. To avoid this chain, we use l_linenumber.

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem WHERE l_orderkey > 9030;

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem
	WHERE (l_orderkey < 4000 OR l_orderkey > 9030);

-- The following query should prune out all shards and return empty results

SELECT sum(l_linenumber), avg(l_linenumber) FROM lineitem WHERE l_orderkey > 20000;

-- The tests below verify that we can prune shards partitioned over different
-- types of columns including varchar, array types, composite types etc. This is
-- in response to a bug we had where we were not able to resolve correct operator
-- types for some kind of column types. First we create tables partitioned on
-- these types and the logical shards and placements for them.

-- Create varchar partitioned table

CREATE TABLE varchar_partitioned_table
(
	varchar_column varchar(100)
);
SELECT master_create_distributed_table('varchar_partitioned_table', 'varchar_column', 'append');

-- Create logical shards and shard placements with shardid 100,101

INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	VALUES('varchar_partitioned_table'::regclass, 100, 't', 'AA1000U2AMO4ZGX', 'AZZXSP27F21T6'),
		  ('varchar_partitioned_table'::regclass, 101, 't', 'BA1000U2AMO4ZGX', 'BZZXSP27F21T6');

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 100, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 101, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

-- Create array partitioned table

RESET client_min_messages; -- avoid debug messages about toast index creation
CREATE TABLE array_partitioned_table
(
	array_column text[]
);
SELECT master_create_distributed_table('array_partitioned_table', 'array_column', 'append');
SET client_min_messages TO DEBUG2;

-- Create logical shard with shardid 102, 103

INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	VALUES('array_partitioned_table'::regclass, 102, 't', '{}', '{AZZXSP27F21T6, AZZXSP27F21T6}'),
		  ('array_partitioned_table'::regclass, 103, 't', '{BA1000U2AMO4ZGX, BZZXSP27F21T6}',
		   '{CA1000U2AMO4ZGX, CZZXSP27F21T6}');

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 102, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 103, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

-- Create composite type partitioned table

CREATE TYPE composite_type AS
(
	text_column text,
	double_column decimal,
	varchar_column varchar(50)
);

RESET client_min_messages; -- avoid debug messages about toast index creation
CREATE TABLE composite_partitioned_table
(
	composite_column composite_type
);
SELECT master_create_distributed_table('composite_partitioned_table', 'composite_column', 'append');
SET client_min_messages TO DEBUG2;

-- Create logical shard with shardid 104, 105
INSERT INTO pg_dist_shard (logicalrelid, shardid, shardstorage, shardminvalue, shardmaxvalue)
	VALUES('composite_partitioned_table'::regclass, 104, 't', '(a,3,b)', '(b,4,c)'),
		  ('composite_partitioned_table'::regclass, 105, 't', '(c,5,d)', '(d,6,e)');

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 104, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

INSERT INTO pg_dist_shard_placement (shardid, shardstate, shardlength, nodename, nodeport)
	SELECT 105, 1, 1, nodename, nodeport
	FROM pg_dist_shard_placement
	GROUP BY nodename, nodeport
	ORDER BY nodename, nodeport ASC
	LIMIT 1;

-- Verify that shard pruning works. Note that these queries should all prune
-- one shard.

EXPLAIN SELECT count(*) FROM varchar_partitioned_table WHERE varchar_column = 'BA2';

EXPLAIN SELECT count(*) FROM array_partitioned_table
	WHERE array_column > '{BA1000U2AMO4ZGX, BZZXSP27F21T6}';

EXPLAIN SELECT count(*) FROM composite_partitioned_table
	WHERE composite_column < '(b,5,c)'::composite_type;

SET client_min_messages TO NOTICE;
