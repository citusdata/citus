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
DROP SCHEMA detach_partition_concurrently;
