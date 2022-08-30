--
-- failure_insert_select_repartition
--
-- performs failure/cancellation test for repartitioned insert/select.
--

CREATE SCHEMA repartitioned_insert_select;
SET SEARCH_PATH=repartitioned_insert_select;
SELECT pg_backend_pid() as pid \gset

SET citus.next_shard_id TO 4213581;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 2;
SET citus.max_adaptive_executor_pool_size TO 1; -- have more consistent number of connections running
CREATE TABLE replicated_source_table(a int, b int);
SELECT create_distributed_table('replicated_source_table', 'a');
INSERT INTO replicated_source_table SELECT i, i*i FROM generate_series(1, 10) i;

SET citus.shard_count TO 3;
CREATE TABLE replicated_target_table(a int, b int);
SELECT create_distributed_table('replicated_target_table', 'a');

SET citus.shard_replication_factor TO 1;

SET citus.shard_count TO 4;
CREATE TABLE source_table(a int, b int);
SELECT create_distributed_table('source_table', 'a');
INSERT INTO source_table SELECT i, i*i FROM generate_series(1, 10) i;

SET citus.shard_count TO 3;
CREATE TABLE target_table(a int, b int);
SELECT create_distributed_table('target_table', 'a');

--
-- kill worker_partition_query_result
-- this fails the query on source table, so replicated case should succeed
--
SELECT citus.mitmproxy('conn.onQuery(query="worker_partition_query_result").kill()');
INSERT INTO target_table SELECT * FROM source_table;

SELECT * FROM target_table ORDER BY a;

SELECT citus.mitmproxy('conn.onQuery(query="worker_partition_query_result").kill()');
INSERT INTO target_table SELECT * FROM replicated_source_table;

SELECT * FROM target_table ORDER BY a;

--
-- kill the COPY command that's created by fetch_intermediate_results
-- this fails the fetch into target, so source replication doesn't matter
-- and both should fail
-- We don't kill the fetch_intermediate_results query directly, because that
-- resulted in randomly failing tests on CI. The reason for that is that there
-- is a race condition, where killing the fetch_intermediate_results query
-- removes the data files before the fetch_intermediate_results query from the
-- other node can read them. In theory a similar race condition still exists
-- when killing the COPY, but CI doesn't hit that race condition in practice.
--

TRUNCATE target_table;

SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
INSERT INTO target_table SELECT * FROM source_table;

SELECT * FROM target_table ORDER BY a;

SELECT citus.mitmproxy('conn.onQuery(query="COPY").kill()');
INSERT INTO target_table SELECT * FROM replicated_source_table;

SELECT * FROM target_table ORDER BY a;

--
-- kill read_intermediate_results
-- again, both should fail
--

TRUNCATE target_table;

SELECT citus.mitmproxy('conn.onQuery(query="read_intermediate_results").kill()');
INSERT INTO target_table SELECT * FROM source_table;

SELECT * FROM target_table ORDER BY a;

SELECT citus.mitmproxy('conn.onQuery(query="read_intermediate_results").kill()');
INSERT INTO target_table SELECT * FROM replicated_source_table;

SELECT * FROM target_table ORDER BY a;

--
-- We error out even if table is replicated and only one of the replicas
-- fail.
--

SELECT citus.mitmproxy('conn.onQuery(query="read_intermediate_results").kill()');
INSERT INTO replicated_target_table SELECT * FROM source_table;

SELECT * FROM replicated_target_table;

RESET SEARCH_PATH;
SELECT citus.mitmproxy('conn.allow()');
\set VERBOSITY TERSE
DROP SCHEMA repartitioned_insert_select CASCADE;
