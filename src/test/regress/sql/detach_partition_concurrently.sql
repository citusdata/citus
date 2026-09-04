CREATE SCHEMA detach_partition_concurrently;
SET search_path TO detach_partition_concurrently;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

CREATE TABLE parent (a int) PARTITION BY RANGE (a);
CREATE TABLE child PARTITION OF parent FOR VALUES FROM (0) TO (10);
SELECT create_distributed_table('parent', 'a');

ALTER TABLE parent DETACH PARTITION child CONCURRENTLY;

SELECT relispartition
FROM pg_class
WHERE oid = 'child'::regclass;

SELECT result
FROM run_command_on_workers($$
	SELECT count(*)
	FROM pg_inherits i
	JOIN pg_class p ON p.oid = i.inhparent
	WHERE p.relname LIKE 'parent\_%'
$$)
ORDER BY result;

INSERT INTO child VALUES (1);
SELECT * FROM child;

DROP TABLE child;
DROP TABLE parent;

-- A retry must reconcile workers that completed different amounts of work.
CREATE TABLE retry_parent (a int) PARTITION BY RANGE (a);
CREATE TABLE retry_child PARTITION OF retry_parent FOR VALUES FROM (0) TO (10);
SELECT create_distributed_table('retry_parent', 'a');

SELECT parent_shard.shardid AS parent_shard_id,
	   child_shard.shardid AS child_shard_id
FROM pg_dist_shard parent_shard
JOIN pg_dist_shard child_shard
	ON child_shard.shardminvalue = parent_shard.shardminvalue
	AND child_shard.shardmaxvalue = parent_shard.shardmaxvalue
JOIN pg_dist_placement placement
	ON placement.shardid = parent_shard.shardid
JOIN pg_dist_node node
	ON node.groupid = placement.groupid
WHERE parent_shard.logicalrelid = 'retry_parent'::regclass
	AND child_shard.logicalrelid = 'retry_child'::regclass
	AND node.nodeport = :worker_1_port
\gset

\c - - - :worker_1_port
ALTER TABLE detach_partition_concurrently.retry_parent_:parent_shard_id
	DETACH PARTITION detach_partition_concurrently.retry_child_:child_shard_id
	CONCURRENTLY;

\c - - - :master_port
SET search_path TO detach_partition_concurrently;
ALTER TABLE retry_parent DETACH PARTITION retry_child CONCURRENTLY;

SELECT result
FROM run_command_on_workers($$
	SELECT count(*)
	FROM pg_inherits i
	JOIN pg_class p ON p.oid = i.inhparent
	WHERE p.relname LIKE 'retry_parent\_%'
$$)
ORDER BY result;

DROP TABLE retry_child;
DROP TABLE retry_parent;
DROP SCHEMA detach_partition_concurrently;
