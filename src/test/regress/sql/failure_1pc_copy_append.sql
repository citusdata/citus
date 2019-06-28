SELECT citus.mitmproxy('conn.allow()');

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 1;

SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- one shard per worker
SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.next_shard_id TO 100400;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100;

CREATE TABLE copy_test (key int, value int);
SELECT create_distributed_table('copy_test', 'key', 'append');

SELECT citus.clear_network_traffic();

COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT count(1) FROM copy_test;

SELECT citus.dump_network_traffic();

---- all of the following tests test behavior with 2 shard placements ----
SHOW citus.shard_replication_factor;

---- kill the connection when we try to create the shard ----
SELECT citus.mitmproxy('conn.onQuery(query="worker_apply_shard_ddl_command").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we try to start a transaction ----
SELECT citus.mitmproxy('conn.onQuery(query="assign_distributed_transaction_id").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we start the COPY ----
SELECT citus.mitmproxy('conn.onQuery(query="FROM STDIN WITH").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we send the data ----
SELECT citus.mitmproxy('conn.onCopyData().kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;

SELECT citus.mitmproxy('conn.onQuery(query="SELECT|COPY").kill()');
SELECT count(1) FROM copy_test;

---- cancel the connection when we send the data ----
SELECT citus.mitmproxy('conn.onQuery(query="SELECT|COPY").cancel(' ||  pg_backend_pid() || ')');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we try to get the size of the table ----
SELECT citus.mitmproxy('conn.onQuery(query="pg_table_size").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

-- we round-robin when picking which node to run pg_table_size on, this COPY runs it on
-- the other node, so the next copy will try to run it on our node
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY p.nodeport, p.placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we try to get the min, max of the table ----
SELECT citus.mitmproxy('conn.onQuery(query="SELECT min\(key\), max\(key\)").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

---- kill the connection when we try to COMMIT ----
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
COPY copy_test FROM PROGRAM 'echo 0, 0 && echo 1, 1 && echo 2, 4 && echo 3, 9' WITH CSV;
SELECT * FROM pg_dist_shard s, pg_dist_shard_placement p
  WHERE (s.shardid = p.shardid) AND s.logicalrelid = 'copy_test'::regclass
  ORDER BY placementid;
SELECT count(1) FROM copy_test;

-- ==== Clean up, we're done here ====

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE copy_test;
