SET citus.next_shard_id TO 100500;

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE ref_table (key int, value int);
SELECT create_reference_table('ref_table');

\copy ref_table FROM stdin delimiter ',';
1,2
2,3
3,4
4,5
\.

SELECT citus.clear_network_traffic();

SELECT COUNT(*) FROM ref_table;

-- verify behavior of single INSERT; should fail to execute
SELECT citus.mitmproxy('conn.onQuery(query="INSERT").kill()');
INSERT INTO ref_table VALUES (5, 6);

SELECT COUNT(*) FROM ref_table WHERE key=5;

-- verify behavior of UPDATE ... RETURNING; should not execute
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").kill()');
UPDATE ref_table SET key=7 RETURNING value;

SELECT COUNT(*) FROM ref_table WHERE key=7;

-- verify fix to #2214; should raise error and fail to execute
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").kill()');

BEGIN;
DELETE FROM ref_table WHERE key=5;
UPDATE ref_table SET key=value;
COMMIT;

SELECT COUNT(*) FROM ref_table WHERE key=value;

-- all shards should still be healthy
SELECT COUNT(*) FROM pg_dist_shard_placement WHERE shardstate = 3;

-- ==== Clean up, we're done here ====

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE ref_table;
