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
SELECT citus.mitmproxy('conn.onQuery(query="DELETE").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes performed in failed transaction
SELECT * FROM dml_test ORDER BY id ASC;

-- cancel at DELETE
SELECT citus.mitmproxy('conn.onQuery(query="DELETE").cancel(' ||  pg_backend_pid() || ')');

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
SELECT citus.mitmproxy('conn.onQuery(query="INSERT").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes before failed INSERT
SELECT * FROM dml_test ORDER BY id ASC;

-- cancel at INSERT
SELECT citus.mitmproxy('conn.onQuery(query="INSERT").cancel(' ||  pg_backend_pid() || ')');

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
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").kill()');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes after failed UPDATE
SELECT * FROM dml_test ORDER BY id ASC;

-- cancel at UPDATE
SELECT citus.mitmproxy('conn.onQuery(query="UPDATE").cancel(' ||  pg_backend_pid() || ')');

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

-- this transaction block will be sent to the coordinator as a remote command to hide the
-- error message that is caused during commit.
-- we'll test for the txn side-effects to ensure it didn't run
SELECT master_run_on_worker(
    ARRAY['localhost']::text[],
    ARRAY[:master_port]::int[],
    ARRAY['
BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, ''Epsilon'');
UPDATE dml_test SET name = ''alpha'' WHERE id = 1;
UPDATE dml_test SET name = ''gamma'' WHERE id = 3;
COMMIT;
    '],
    false
);

SELECT citus.mitmproxy('conn.allow()');
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;
SELECT recover_prepared_transactions();

-- shouldn't see any changes after failed PREPARE
SELECT * FROM dml_test ORDER BY id ASC;


-- cancel at PREPARE TRANSACTION
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');

-- we'll test for the txn side-effects to ensure it didn't run
BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

SELECT citus.mitmproxy('conn.allow()');
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;
SELECT recover_prepared_transactions();

-- shouldn't see any changes after failed PREPARE
SELECT * FROM dml_test ORDER BY id ASC;

-- fail at COMMIT
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');

-- hide the error message (it has the PID)...
-- we'll test for the txn side-effects to ensure it didn't run
SET client_min_messages TO ERROR;

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


-- cancel at COMMITs are ignored by Postgres
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").cancel(' ||  pg_backend_pid() || ')');


BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

-- should see changes, because cancellation is ignored
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

-- fail at PREPARED COMMIT as we use 2PC
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
SET client_min_messages TO ERROR;

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

RESET client_min_messages;

-- all changes should be committed because we injected
-- the failure on the COMMIT time. And, we should not
-- mark any placements as INVALID
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT shardid FROM pg_dist_shard_placement WHERE shardstate = 3;

SET citus.task_assignment_policy TO "round-robin";
SELECT * FROM dml_test ORDER BY id ASC;
SELECT * FROM dml_test ORDER BY id ASC;
RESET citus.task_assignment_policy;

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

-- cancel at COMMIT (by cancelling on PREPARE)
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE").cancel(' ||  pg_backend_pid() || ')');

BEGIN;
DELETE FROM dml_test WHERE id = 1;
DELETE FROM dml_test WHERE id = 2;
INSERT INTO dml_test VALUES (5, 'Epsilon');
UPDATE dml_test SET name = 'alpha' WHERE id = 1;
UPDATE dml_test SET name = 'gamma' WHERE id = 3;
COMMIT;

--- shouldn't see any changes after cancelled PREPARE
SELECT * FROM dml_test ORDER BY id ASC;

-- allow connection to allow DROP
SELECT citus.mitmproxy('conn.allow()');
DROP TABLE dml_test;
