--
-- Distributed Partitioned Table Tests
--
SET citus.next_shard_id TO 1760000;

CREATE SCHEMA partitioned_table_replicated;
SET search_path TO partitioned_table_replicated;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 2;

CREATE TABLE collections (
	key bigint,
	ts timestamptz,
	collection_id integer,
	value numeric
) PARTITION BY LIST ( collection_id );


CREATE TABLE collections_1
	PARTITION OF collections (key, ts, collection_id, value)
	FOR VALUES IN ( 1 );

CREATE TABLE collections_2
	PARTITION OF collections (key, ts, collection_id, value)
	FOR VALUES IN ( 2 );

-- load some data data
INSERT INTO collections (key, ts, collection_id, value) VALUES (1, '2009-01-01', 1, 1);
INSERT INTO collections (key, ts, collection_id, value) VALUES (2, '2009-01-01', 1, 2);
INSERT INTO collections (key, ts, collection_id, value) VALUES (3, '2009-01-01', 2, 1);
INSERT INTO collections (key, ts, collection_id, value) VALUES (4, '2009-01-01', 2, 2);

-- in the first case, we'll distributed the
-- already existing partitioninong hierarcy
SELECT create_distributed_table('collections', 'key');

-- now create partition of a already distributed table
CREATE TABLE collections_3 PARTITION OF collections FOR VALUES IN ( 3 );

-- now attaching non distributed table to a distributed table
CREATE TABLE collections_4 AS SELECT * FROM collections LIMIT 0;

-- load some data
INSERT INTO collections_4 SELECT i, '2009-01-01', 4, i FROM generate_series (0, 10) i;

ALTER TABLE collections ATTACH PARTITION collections_4 FOR VALUES IN ( 4 );

-- finally attach a distributed table to a distributed table
CREATE TABLE collections_5 AS SELECT * FROM collections LIMIT 0;
SELECT create_distributed_table('collections_5', 'key');

-- load some data
INSERT INTO collections_5 SELECT i, '2009-01-01', 5, i FROM generate_series (0, 10) i;
ALTER TABLE collections ATTACH PARTITION collections_5 FOR VALUES IN ( 5 );

-- make sure that we've all the placements
SELECT
	logicalrelid, count(*) as placement_count
FROM
	pg_dist_shard, pg_dist_shard_placement
WHERE
	logicalrelid::text LIKE '%collections%' AND
	pg_dist_shard.shardid = pg_dist_shard_placement.shardid
GROUP BY
	logicalrelid
ORDER BY
	1,2;

-- and, make sure that all tables are colocated
SELECT
	count(DISTINCT colocationid)
FROM
	pg_dist_partition
WHERE
	logicalrelid::text LIKE '%collections%';

-- make sure that any kind of modification is disallowed on partitions
-- given that replication factor > 1
INSERT INTO collections_4 (key, ts, collection_id, value) VALUES (4, '2009-01-01', 2, 2);

-- single shard update/delete not allowed
UPDATE collections_1 SET ts = now() WHERE key = 1;
DELETE FROM collections_1 WHERE ts = now() AND key = 1;

-- multi shard update/delete are not allowed
UPDATE collections_1 SET ts = now();
DELETE FROM collections_1 WHERE ts = now();

-- insert..select pushdown
INSERT INTO collections_1 SELECT * FROM collections_1;

-- insert..select via coordinator
INSERT INTO collections_1 SELECT * FROM collections_1 OFFSET 0;

-- COPY is not allowed
COPY collections_1 FROM STDIN;
\.

-- DDLs are not allowed
CREATE INDEX index_on_partition ON collections_1(key);

-- EXPLAIN with modifications is not allowed as well
UPDATE collections_1 SET ts = now() WHERE key = 1;

-- TRUNCATE is also not allowed
TRUNCATE collections_1;
TRUNCATE collections, collections_1;

-- modifying CTEs are also not allowed
WITH collections_5_cte AS
(
	DELETE FROM collections_5 RETURNING *
)
SELECT * FROM collections_5_cte;

-- foreign key creation is disallowed due to replication factor > 1
CREATE TABLE fkey_test (key bigint PRIMARY KEY);
SELECT create_distributed_table('fkey_test', 'key');

ALTER TABLE
	collections_5
ADD CONSTRAINT
	fkey_delete FOREIGN KEY(key)
REFERENCES
	fkey_test(key) ON DELETE CASCADE;

-- we should be able to attach and detach partitions
-- given that those DDLs are on the parent table
CREATE TABLE collections_6
	PARTITION OF collections (key, ts, collection_id, value)
	FOR VALUES IN ( 6 );

ALTER TABLE collections DETACH PARTITION collections_6;
ALTER TABLE collections ATTACH PARTITION collections_6 FOR VALUES IN ( 6 );

-- read queries works just fine
SELECT count(*) FROM collections_1 WHERE key = 1;
SELECT count(*) FROM collections_1 WHERE key != 1;

-- rollups SELECT'ing from partitions should work just fine
CREATE TABLE collections_agg (
	key bigint,
	sum_value numeric
);

SELECT create_distributed_table('collections_agg', 'key');

-- pushdown roll-up
INSERT INTO collections_agg SELECT key, sum(key) FROM collections_1 GROUP BY key;

-- coordinator roll-up
INSERT INTO collections_agg SELECT collection_id, sum(key) FROM collections_1 GROUP BY collection_id;

-- now make sure that copy functionality works fine
-- create a table and create its distribution metadata
CREATE TABLE customer_engagements (id integer, event_id int) PARTITION BY LIST ( event_id );

CREATE TABLE customer_engagements_1
	PARTITION OF customer_engagements
	FOR VALUES IN ( 1 );

CREATE TABLE customer_engagements_2
	PARTITION OF customer_engagements
	FOR VALUES IN ( 2 );

-- add some indexes
CREATE INDEX ON customer_engagements (id);
CREATE INDEX ON customer_engagements (event_id);
CREATE INDEX ON customer_engagements (id, event_id);

-- distribute the table
-- create a single shard on the first worker
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 2;
SELECT create_distributed_table('customer_engagements', 'id', 'hash', colocate_with := 'none');

-- ingest some data for the tests
INSERT INTO customer_engagements VALUES (1, 1);
INSERT INTO customer_engagements VALUES (2, 1);
INSERT INTO customer_engagements VALUES (1, 2);
INSERT INTO customer_engagements VALUES (2, 2);

-- get the newshardid
SELECT shardid as newshardid FROM pg_dist_shard WHERE logicalrelid = 'customer_engagements'::regclass
\gset

-- delete all the placements on the second node belonging to partitioning hierarchy
DELETE FROM pg_dist_shard_placement p USING pg_dist_shard s
WHERE s.shardid = p.shardid AND nodeport = :worker_2_port AND logicalrelid::text LIKE 'customer_engagements%';

-- cannot copy a shard after a modification (transaction still open during copy)
BEGIN;
INSERT INTO customer_engagements VALUES (1, 1);
SELECT citus_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, transfer_mode := 'block_writes');
ROLLBACK;

-- modifications after copying a shard are fine (will use new metadata)
BEGIN;
SELECT citus_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, transfer_mode := 'block_writes');
ALTER TABLE customer_engagements ADD COLUMN value float DEFAULT 1.0;
SELECT * FROM customer_engagements ORDER BY 1,2,3;
ROLLBACK;

BEGIN;
SELECT citus_copy_shard_placement(:newshardid, 'localhost', :worker_1_port, 'localhost', :worker_2_port, transfer_mode := 'block_writes');
INSERT INTO customer_engagements VALUES (1, 1);
SELECT count(*) FROM customer_engagements;
ROLLBACK;

-- TRUNCATE is allowed on the parent table
-- try it just before dropping the table
TRUNCATE collections;

SET search_path TO public;
DROP SCHEMA partitioned_table_replicated CASCADE;
