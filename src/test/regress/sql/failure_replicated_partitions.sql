SET citus.next_shard_id TO 1490000;

SELECT citus.mitmproxy('conn.allow()');


SET citus.shard_replication_factor TO 2;
SET "citus.replication_model" to "statement";
SET citus.shard_count TO 4;

CREATE TABLE partitioned_table (
	dist_key bigint,
	partition_id integer
) PARTITION BY LIST (partition_id );

SELECT create_distributed_table('partitioned_table', 'dist_key');
CREATE TABLE partitioned_table_0
	PARTITION OF partitioned_table (dist_key, partition_id)
	FOR VALUES IN ( 0 );


INSERT INTO partitioned_table VALUES (0, 0);

SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").kill()');

INSERT INTO partitioned_table VALUES (0, 0);

-- use both placements
SET citus.task_assignment_policy TO "round-robin";

-- the results should be the same
SELECT count(*) FROM partitioned_table_0;
SELECT count(*) FROM partitioned_table_0;

SELECT count(*) FROM partitioned_table;
SELECT count(*) FROM partitioned_table;


-- ==== Clean up, we're done here ====

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE partitioned_table;

