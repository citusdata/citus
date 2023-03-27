SELECT citus.mitmproxy('conn.allow()');
SELECT citus.clear_network_traffic();

SET citus.shard_count = 2;
SET citus.shard_replication_factor = 2;

-- this test is designed such that no modification lock is acquired
SET citus.allow_modifications_from_workers_to_replicated_tables TO false;

CREATE TABLE select_test (key int, value text);
SELECT create_distributed_table('select_test', 'key');

-- put data in shard for which mitm node is first placement
INSERT INTO select_test VALUES (3, 'test data');

SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").kill()');
SELECT * FROM select_test WHERE key = 3;
SELECT * FROM select_test WHERE key = 3;

-- kill after first SELECT; txn should fail as INSERT triggers
-- 2PC (and placementis not marked bad)
SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").kill()');

BEGIN;
INSERT INTO select_test VALUES (3, 'more data');
SELECT * FROM select_test WHERE key = 3;
COMMIT;

SELECT citus.mitmproxy('conn.allow()');

TRUNCATE select_test;

-- now the same tests with query cancellation

-- put data in shard for which mitm node is first placement
INSERT INTO select_test VALUES (3, 'test data');

SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").cancel(' ||  pg_backend_pid() || ')');
SELECT * FROM select_test WHERE key = 3;
SELECT * FROM select_test WHERE key = 3;

-- cancel after first SELECT; txn should fail and nothing should be marked as invalid
SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").cancel(' ||  pg_backend_pid() || ')');

BEGIN;
INSERT INTO select_test VALUES (3, 'more data');
SELECT * FROM select_test WHERE key = 3;
COMMIT;

-- show that all placements are OK
SELECT DISTINCT shardstate FROM  pg_dist_shard_placement
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'select_test'::regclass
);

SELECT citus.mitmproxy('conn.allow()');
TRUNCATE select_test;

-- cancel the second query
-- error after second SELECT; txn should fail
SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").after(1).cancel(' ||  pg_backend_pid() || ')');

BEGIN;
INSERT INTO select_test VALUES (3, 'more data');
SELECT * FROM select_test WHERE key = 3;
INSERT INTO select_test VALUES (3, 'even more data');
SELECT * FROM select_test WHERE key = 3;
COMMIT;

-- error after second SELECT; txn should fails the transaction
SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").after(1).reset()');

BEGIN;
INSERT INTO select_test VALUES (3, 'more data');
SELECT * FROM select_test WHERE key = 3;
INSERT INTO select_test VALUES (3, 'even more data');
SELECT * FROM select_test WHERE key = 3;
COMMIT;

SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*pg_prepared_xacts").after(2).kill()');
SELECT recover_prepared_transactions();
SELECT recover_prepared_transactions();

-- bug from https://github.com/citusdata/citus/issues/1926
SET citus.max_cached_conns_per_worker TO 0; -- purge cache
DROP TABLE select_test;
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;

CREATE TABLE select_test (key int, value text);
SELECT create_distributed_table('select_test', 'key');

SET citus.max_cached_conns_per_worker TO 1; -- allow connection to be cached
INSERT INTO select_test VALUES (1, 'test data');

SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").after(1).kill()');
SELECT * FROM select_test WHERE key = 1;
SELECT * FROM select_test WHERE key = 1;

-- now the same test with query cancellation
SELECT citus.mitmproxy('conn.onQuery(query="SELECT.*select_test").after(1).cancel(' ||  pg_backend_pid() || ')');
SELECT * FROM select_test WHERE key = 1;
SELECT * FROM select_test WHERE key = 1;

-- ==== Clean up, we're done here ====

DROP TABLE select_test;
