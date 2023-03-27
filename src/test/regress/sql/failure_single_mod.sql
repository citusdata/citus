SELECT citus.mitmproxy('conn.allow()');
SELECT citus.clear_network_traffic();

SET citus.shard_count = 2;
SET citus.shard_replication_factor = 2;

CREATE TABLE mod_test (key int, value text);
SELECT create_distributed_table('mod_test', 'key');

-- verify behavior of single INSERT; should mark shard as failed
SELECT citus.mitmproxy('conn.onQuery(query="INSERT").kill()');
INSERT INTO mod_test VALUES (2, 6);

SELECT COUNT(*) FROM mod_test WHERE key=2;

-- none of the placements are marked as INACTIVE
UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'mod_test'::regclass
) AND shardstate = 3 RETURNING placementid;
TRUNCATE mod_test;

-- verify behavior of UPDATE ... RETURNING; should fail the transaction
SELECT citus.mitmproxy('conn.allow()');
INSERT INTO mod_test VALUES (2, 6);

SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").kill()');
UPDATE mod_test SET value='ok' WHERE key=2 RETURNING key;

SELECT COUNT(*) FROM mod_test WHERE value='ok';

-- none of the placements are marked as INACTIVE
UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'mod_test'::regclass
) AND shardstate = 3 RETURNING placementid;
TRUNCATE mod_test;

-- verify behavior of multi-statement modifications to a single shard
-- should fail the transaction and never mark placements inactive
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").kill()');

BEGIN;
INSERT INTO mod_test VALUES (2, 6);
INSERT INTO mod_test VALUES (2, 7);
DELETE FROM mod_test WHERE key=2 AND value = '7';
UPDATE mod_test SET value='ok' WHERE key=2;
COMMIT;

SELECT COUNT(*) FROM mod_test WHERE key=2;

-- none of the placements are marked as INACTIVE
UPDATE pg_dist_shard_placement SET shardstate = 1
WHERE shardid IN (
  SELECT shardid FROM pg_dist_shard WHERE logicalrelid = 'mod_test'::regclass
) AND shardstate = 3 RETURNING placementid;
TRUNCATE mod_test;

-- ==== Clean up, we're done here ====

DROP TABLE mod_test;
