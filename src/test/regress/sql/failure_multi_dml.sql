SELECT citus.mitmproxy('conn.allow()');

SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1; -- one shard per worker
SET citus.next_shard_id TO 103400;
ALTER SEQUENCE pg_catalog.pg_dist_placement_placementid_seq RESTART 100;

CREATE TABLE dml_test (id integer, name text);
SELECT create_distributed_table('dml_test', 'id');

COPY dml_test FROM STDIN WITH CSV;
1,Alpha
2,Beta
3,Gamma
4,Delta
\.

SELECT citus.clear_network_traffic();

---- test multiple statements spanning multiple shards,
---- at each significant point. These transactions are 2pc

-- fail at DELETE
SELECT citus.mitmproxy('conn.onQuery(query="^DELETE").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes performed in failed transaction
SELECT * FROM dml_test ORDER BY id ASC;

-- fail at INSERT
SELECT citus.mitmproxy('conn.onQuery(query="^INSERT").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes before failed INSERT
SELECT * FROM dml_test ORDER BY id ASC;

-- fail at UPDATE
SELECT citus.mitmproxy('conn.onQuery(query="^UPDATE").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes after failed UPDATE
SELECT * FROM dml_test ORDER BY id ASC;

-- fail at PREPARE TRANSACTION
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").kill()');

-- hide the error message (it has the PID)...
-- we'll test for the txn side-effects to ensure it didn't run
SET client_min_messages TO FATAL;

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

SET client_min_messages TO DEFAULT;

SELECT citus.mitmproxy('conn.allow()');
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;
SELECT recover_prepared_transactions();

-- shouldn't see any changes after failed PREPARE
SELECT * FROM dml_test ORDER BY id ASC;

-- fail at COMMIT
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');

-- hide the error message (it has the PID)...
-- we'll test for the txn side-effects to ensure it didn't run
SET client_min_messages TO FATAL;

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

SET client_min_messages TO DEFAULT;

SELECT citus.mitmproxy('conn.allow()');
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;
SELECT recover_prepared_transactions();

-- should see changes, because of txn recovery
SELECT * FROM dml_test ORDER BY id ASC;

-- drop table and recreate with different replication/sharding

DROP TABLE dml_test;
SET citus.shard_count = 1;
SET citus.shard_replication_factor = 2; -- two placements

CREATE TABLE dml_test (id integer, name text);
SELECT create_distributed_table('dml_test', 'id');

COPY dml_test FROM STDIN WITH CSV;
1,Alpha
2,Beta
3,Gamma
4,Delta
\.

---- test multiple statements against a single shard, but with two placements

-- fail at COMMIT (actually COMMIT this time, as no 2pc in use)
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- should see all changes, but they only went to one placement (other is unhealthy)
SELECT * FROM dml_test ORDER BY id ASC;
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;

SELECT citus.mitmproxy('conn.allow()');

-- drop table and recreate as reference table

DROP TABLE dml_test;
SET citus.shard_count = 2;
SET citus.shard_replication_factor = 1;

CREATE TABLE dml_test (id integer, name text);
SELECT create_reference_table('dml_test');

COPY dml_test FROM STDIN WITH CSV;
1,Alpha
2,Beta
3,Gamma
4,Delta
\.

-- fail at COMMIT (by failing to PREPARE)
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes after failed COMMIT
SELECT * FROM dml_test ORDER BY id ASC;

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE dml_test;
