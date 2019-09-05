-- We have different output files for the executor. This is because
-- we don't mark transactions with ANALYZE as critical anymore, and
-- get WARNINGs instead of ERRORs.

-- print whether we're using version > 10 to make version-specific tests clear
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int > 10 AS version_above_ten;

SET citus.next_shard_id TO 12000000;

SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- one shard per worker
SET citus.multi_shard_commit_protocol TO '1pc';

CREATE TABLE vacuum_test (key int, value int);
SELECT create_distributed_table('vacuum_test', 'key');

SELECT citus.clear_network_traffic();

SELECT citus.mitmproxy('conn.onQuery(query="^VACUUM").kill()');
VACUUM vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^ANALYZE").kill()');
ANALYZE vacuum_test;

SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
ANALYZE vacuum_test;

-- ANALYZE transactions being critical is an open question, see #2430
-- show that we marked as INVALID on COMMIT FAILURE
SELECT shardid, shardstate FROM pg_dist_shard_placement where shardstate != 1 AND 
shardid in ( SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'vacuum_test'::regclass);

UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'vacuum_test'::regclass
);

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
