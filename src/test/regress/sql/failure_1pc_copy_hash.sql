SELECT citus.mitmproxy('conn.allow()');

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 0;

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- one shard per worker
SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.next_shard_id TO 100400;
SET citus.max_cached_conns_per_worker TO 0;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100;

CREATE TABLE copy_test (key int, value int);
SELECT create_distributed_table('copy_test', 'key');

SELECT citus.clear_network_traffic();

COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT count(1) FROM copy_test;

SELECT citus.dump_network_traffic();

-- ==== kill the connection when we try to start a transaction ====
-- the query should abort

SELECT citus.mitmproxy('conn.onQuery(query="assign_distributed_transaction").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

-- ==== kill the connection when we try to start the COPY ====
-- the query should abort

SELECT citus.mitmproxy('conn.onQuery(query="FROM STDIN WITH").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

-- ==== kill the connection when we first start sending data ====
-- the query should abort

SELECT citus.mitmproxy('conn.onCopyData().killall()'); -- raw rows from the client
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

-- ==== kill the connection when the worker confirms it's received the data ====
-- the query should abort

SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

-- ==== kill the connection when we try to send COMMIT ====
-- the query should succeed, and the placement should be marked inactive

SELECT citus.mitmproxy('conn.allow()');
SELECT count(1) FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) AND shardstate = 3;
SELECT count(1) FROM copy_test;

SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT$").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;

-- the shard is marked invalid
SELECT citus.mitmproxy('conn.allow()');
SELECT count(1) FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) AND shardstate = 3;
SELECT count(1) FROM copy_test;

-- ==== clean up a little bit before running the next test ====

UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
);
TRUNCATE copy_test;

-- ==== try to COPY invalid data ====

-- here the coordinator actually sends the data, but then unexpectedly closes the
-- connection when it notices the data stream is broken. Crucially, it closes the
-- connection before sending COMMIT, so no data makes it into the worker.
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9 && echo 10' WITH CSV;

-- kill the connection if the coordinator sends COMMIT. It doesn't, so nothing changes
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT$").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9 && echo 10' WITH CSV;

SELECT * FROM copy_test ORDER BY key, value;

-- ==== clean up some more to prepare for tests with only one replica ====

SELECT citus.mitmproxy('conn.allow()');

TRUNCATE copy_test;
UPDATE pg_dist_shard_placement SET shardstate = 3 WHERE nodeport = :worker_1_port;
SELECT * FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) ORDER BY nodeport, placementid;

-- ==== okay, run some tests where there's only one active shard ====

COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM copy_test;

-- the worker is unreachable
SELECT citus.mitmproxy('conn.killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM copy_test;

-- the first message fails
SELECT citus.mitmproxy('conn.onQuery(query="assign_distributed_transaction_id").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM copy_test;

-- the COPY message fails
SELECT citus.mitmproxy('conn.onQuery(query="FROM STDIN WITH").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM copy_test;

-- the COPY data fails
SELECT citus.mitmproxy('conn.onCopyData().killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM copy_test;

-- the COMMIT fails
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT$").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM copy_test;

-- the placement is not marked invalid
SELECT * FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) ORDER BY nodeport, placementid;

-- the COMMIT makes it through but the connection dies before we get a response
SELECT citus.mitmproxy('conn.onCommandComplete(command="COMMIT").killall()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT citus.mitmproxy('conn.allow()');

SELECT * FROM pg_dist_shard_placement WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'copy_test'::regclass
) ORDER BY nodeport, placementid;
SELECT * FROM copy_test;

-- ==== Clean up, we're done here ====

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE copy_test;
