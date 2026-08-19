--
-- LOCAL_PLAN_CACHE_REUSE
--
-- Regression coverage for the local shard plan cache in local_plan_cache.c.
--
-- (a) The shard query behind a prepared statement should be deparsed and
--     planned once, then reused. The dedup lookup in
--     CacheLocalPlanForShardQuery() used to consult the generic plan's job,
--     whose task list is always empty when pruning is deferred, so the lookup
--     always missed: every execution re-planned the shard query and appended
--     another entry to localPlannedStatements.
--
-- (b) A plan cached while the job had a single task must not be reused once the
--     same prepared statement routes rows to more than one shard. A cached
--     INSERT plan carries every VALUES row, so reusing it for a multi-task job
--     would write rows into the wrong shard.

CREATE SCHEMA local_plan_cache_reuse;
SET search_path TO local_plan_cache_reuse;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 1490000;

CREATE TABLE dist_table (key int PRIMARY KEY, value text);
SELECT create_distributed_table('dist_table', 'key');

-- The tests below rely on keys 1 and 5 sharing a shard while key 6 lives on a
-- different one. Assert that up front so a change in the hash function turns
-- into an obvious failure here rather than a confusing one further down.
SELECT get_shard_id_for_distribution_column('dist_table', 1) =
       get_shard_id_for_distribution_column('dist_table', 5) AS keys_1_and_5_share_a_shard,
       get_shard_id_for_distribution_column('dist_table', 1) =
       get_shard_id_for_distribution_column('dist_table', 6) AS keys_1_and_6_share_a_shard;

-- All three keys land on shards local to worker_1, so the statements below take
-- the local execution path.
\c - - - :worker_1_port
SET search_path TO local_plan_cache_reuse;

--
-- (a) the shard query is deparsed and planned once, not once per execution
--
PREPARE cached_count(int) AS SELECT count(*) FROM dist_table WHERE key = $1;

SET client_min_messages TO DEBUG2;

-- Postgres uses a custom plan for the first executions, so no caching happens
-- yet. Once it switches to the generic plan we should see exactly one
-- "Created and cached local plan" message, and every execution after that
-- should report a cache hit instead of planning the shard query again.
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);
EXECUTE cached_count(1);

RESET client_min_messages;

--
-- (b) a plan cached for a single-task multi-row INSERT is not reused once the
--     same statement spreads its rows over several shards
--
PREPARE multi_row_insert(int, int) AS
	INSERT INTO dist_table VALUES ($1, 'first'), ($2, 'second')
	ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;

-- Keys 1 and 5 share a shard, so each of these executions builds a single task
-- and eventually caches a plan for that shard. That cached plan carries both
-- VALUES rows.
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);
EXECUTE multi_row_insert(1, 5);

DELETE FROM dist_table;

-- Same prepared statement, but now the rows route to two different shards, so
-- the job has two tasks and the plan cached above must not be used. If it were,
-- both rows would be written to the shard holding key 1 and the row for key 6
-- would be lost.
EXECUTE multi_row_insert(1, 6);

-- every row is present exactly once ...
SELECT key, value FROM dist_table ORDER BY key;

-- ... and each one is reachable through the shard its key actually hashes to
SELECT key, value FROM dist_table WHERE key = 1;
SELECT key, value FROM dist_table WHERE key = 6;

-- inspect the shard placements directly: one row in each of the two shards
SELECT 1490000 AS shardid, key, value FROM dist_table_1490000
UNION ALL
SELECT 1490002 AS shardid, key, value FROM dist_table_1490002
ORDER BY shardid, key;

\c - - - :master_port
SET client_min_messages TO WARNING;
DROP SCHEMA local_plan_cache_reuse CASCADE;
