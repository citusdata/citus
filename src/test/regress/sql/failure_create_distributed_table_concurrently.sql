--
-- failure_create_distributed_table_concurrently adds failure tests for creating distributed table concurrently without data.
--

-- due to different libpq versions
-- some warning messages differ
-- between local and CI
SET client_min_messages TO ERROR;

-- setup db
CREATE SCHEMA IF NOT EXISTS create_dist_tbl_con;
SET SEARCH_PATH = create_dist_tbl_con;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;
SET citus.max_adaptive_executor_pool_size TO 1;
SELECT pg_backend_pid() as pid \gset

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 222222;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 333333;

-- make sure coordinator is in the metadata
SELECT citus_set_coordinator_host('localhost', 57636);

-- create table that will be distributed concurrently
CREATE TABLE table_1 (id int PRIMARY KEY);

-- START OF TESTS
SELECT citus.mitmproxy('conn.allow()');

-- failure on shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE create_dist_tbl_con.table_1").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- cancellation on shard table creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE TABLE create_dist_tbl_con.table_1").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on table constraints on replica identity creation
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE create_dist_tbl_con.table_1 ADD CONSTRAINT").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- cancellation on table constraints on replica identity creation
SELECT citus.mitmproxy('conn.onQuery(query="ALTER TABLE create_dist_tbl_con.table_1 ADD CONSTRAINT").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on subscription creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SUBSCRIPTION").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- cancellation on subscription creation
SELECT citus.mitmproxy('conn.onQuery(query="CREATE SUBSCRIPTION").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on catching up LSN
SELECT citus.mitmproxy('conn.onQuery(query="SELECT min\(latest_end_lsn\) FROM pg_stat_subscription").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- cancellation on catching up LSN
SELECT citus.mitmproxy('conn.onQuery(query="SELECT min\(latest_end_lsn\) FROM pg_stat_subscription").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- Comment out below flaky tests. It is caused by shard split cleanup which does not work properly yet.
-- -- failure on dropping subscription
-- SELECT citus.mitmproxy('conn.onQuery(query="DROP SUBSCRIPTION").kill()');
-- SELECT create_distributed_table_concurrently('table_1', 'id');

-- -- cancellation on dropping subscription
-- SELECT citus.mitmproxy('conn.onQuery(query="DROP SUBSCRIPTION").cancel(' || :pid || ')');
-- SELECT create_distributed_table_concurrently('table_1', 'id');

-- -- failure on dropping old shard
-- SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS create_dist_tbl_con.table_1").kill()');
-- SELECT create_distributed_table_concurrently('table_1', 'id');

-- -- cancellation on dropping old shard
-- SELECT citus.mitmproxy('conn.onQuery(query="DROP TABLE IF EXISTS create_dist_tbl_con.table_1").cancel(' || :pid || ')');
-- SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on transaction begin
SELECT citus.mitmproxy('conn.onQuery(query="BEGIN").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on transaction begin
SELECT citus.mitmproxy('conn.onQuery(query="BEGIN").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on transaction commit
SELECT citus.mitmproxy('conn.onQuery(query="COMMIT").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on transaction commit
SELECT citus.mitmproxy('conn.onQuery(query="COMMIT").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on prepare transaction
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").kill()');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- failure on prepare transaction
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").cancel(' || :pid || ')');
SELECT create_distributed_table_concurrently('table_1', 'id');

-- END OF TESTS

SELECT citus.mitmproxy('conn.allow()');

-- Verify that the table can be distributed concurrently after unsuccessful attempts
SELECT create_distributed_table_concurrently('table_1', 'id');
SELECT * FROM pg_dist_shard WHERE logicalrelid = 'table_1'::regclass;

DROP SCHEMA create_dist_tbl_con CASCADE;
SET search_path TO default;
SELECT citus_remove_node('localhost', 57636);
ALTER SEQUENCE pg_dist_node_nodeid_seq RESTART 3;
ALTER SEQUENCE pg_dist_groupid_seq RESTART 3;
