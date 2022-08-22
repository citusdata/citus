-- We have different output files for the executor. This is because
-- we don't mark transactions with ANALYZE as critical anymore, and
-- get WARNINGs instead of ERRORs.

SET citus.next_shard_id TO 12000000;

SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- one shard per worker

CREATE TABLE vacuum_test (key int, value int);
SELECT create_distributed_table('vacuum_test', 'key');

SELECT citus.clear_network_traffic();

SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM").kill()');
VACUUM vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^ANALYZE").kill()');
ANALYZE vacuum_test;

SET client_min_messages TO ERROR;
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
ANALYZE vacuum_test;
RESET client_min_messages;

SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();

-- ANALYZE transactions being critical is an open question, see #2430
-- show that we never mark as INVALID on COMMIT FAILURE
SELECT shardid, shardstate FROM pg_dist_shard_placement where shardstate != 1 AND
shardid in ( SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'vacuum_test'::regclass);

-- the same tests with cancel
SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM").cancel(' ||  pg_backend_pid() || ')');
VACUUM vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^ANALYZE").cancel(' ||  pg_backend_pid() || ')');
ANALYZE vacuum_test;

-- cancel during COMMIT should be ignored
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").cancel(' ||  pg_backend_pid() || ')');
ANALYZE vacuum_test;

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE other_vacuum_test (key int, value int);
SELECT create_distributed_table('other_vacuum_test', 'key');

SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM.*other").kill()');

VACUUM vacuum_test, other_vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM.*other").cancel(' ||  pg_backend_pid() || ')');

VACUUM vacuum_test, other_vacuum_test;

-- ==== Clean up, we're done here ====

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE vacuum_test, other_vacuum_test;
