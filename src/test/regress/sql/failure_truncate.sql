--
-- Test TRUNCATE command failures
--
CREATE SCHEMA truncate_failure;
SET search_path TO 'truncate_failure';
SET citus.next_shard_id TO 120000;
-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO ERROR;

-- do not cache any connections
SET citus.max_cached_conns_per_worker TO 0;

-- use a predictable number of connections per task
SET citus.force_max_query_parallelization TO on;

SELECT citus.mitmproxy('conn.allow()');

-- we'll start with replication factor 1, 2PC and parallel mode
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 1;

CREATE TABLE test_table (key int, value int);
SELECT create_distributed_table('test_table', 'key');

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

CREATE VIEW unhealthy_shard_count AS
  SELECT count(*)
  FROM pg_dist_shard_placement pdsp
  JOIN
  pg_dist_shard pds
  ON pdsp.shardid=pds.shardid
  WHERE logicalrelid='truncate_failure.test_table'::regclass AND shardstate != 1;

-- in the first test, kill just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="TRUNCATE TABLE truncate_failure.test_table").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="TRUNCATE TABLE truncate_failure.test_table").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends COMMIT PREPARED
-- the transaction succeeds on one placement, and we need to
-- recover prepared statements to see the other placement as well
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- refill the table
TRUNCATE test_table;
INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- cancel as soon as the coordinator sends COMMIT
-- interrupts are held during COMMIT/ROLLBACK, so the command
-- should have been applied without any issues since cancel is ignored
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- refill the table
TRUNCATE test_table;
INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

SET client_min_messages TO ERROR;
-- now kill just after the worker sends response to
-- COMMIT command, so we'll have lots of warnings but the command
-- should have been committed both on the distributed table and the placements
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- now cancel just after the worker sends response to
-- but Postgres doesn't accept interrupts during COMMIT and ROLLBACK
-- so should not cancel at all, so not an effective test but adding in
-- case Citus messes up this behaviour
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- Let's test Truncate on reference tables with a FK from a hash distributed table
CREATE TABLE reference_table(i int UNIQUE);
INSERT INTO reference_table SELECT x FROM generate_series(1,20) as f(x);
SELECT create_reference_table('reference_table');

ALTER TABLE test_table ADD CONSTRAINT foreign_key FOREIGN KEY (value) REFERENCES reference_table(i);
-- immediately kill when we see prepare transaction to see if the command
-- still cascaded to referencing table or failed successfuly
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").kill()');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
SELECT count(*) FROM reference_table;

-- immediately cancel when we see prepare transaction to see if the command
-- still cascaded to referencing table or failed successfuly
SELECT citus.mitmproxy('conn.onQuery(query="PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
SELECT count(*) FROM reference_table;

-- immediately kill when we see cascading TRUNCATE on the hash table to see
-- rollbacked properly
SELECT citus.mitmproxy('conn.onQuery(query="^TRUNCATE TABLE").after(2).kill()');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
SELECT count(*) FROM reference_table;

-- immediately cancel when we see cascading TRUNCATE on the hash table to see
-- if the command still cascaded to referencing table or failed successfuly
SELECT citus.mitmproxy('conn.onQuery(query="^TRUNCATE TABLE").after(2).cancel(' ||  pg_backend_pid() || ')');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
SELECT count(*) FROM reference_table;

-- immediately kill after we get prepare transaction complete
-- to see if the command still cascaded to referencing table or
-- failed successfuly
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").kill()');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- immediately cancel after we get prepare transaction complete
-- to see if the command still cascaded to referencing table or
-- failed successfuly
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE reference_table CASCADE;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;


-- in the first test, kill just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="^TRUNCATE TABLE truncate_failure.test_table").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="^TRUNCATE TABLE truncate_failure.test_table").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- killing on PREPARE should be fine, everything should be rollbacked
SELECT citus.mitmproxy('conn.onCommandComplete(command="^PREPARE TRANSACTION").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;

-- we should be able to revocer the transaction and
-- see that the command is rollbacked
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

-- cancelling on PREPARE should be fine, everything should be rollbacked
SELECT citus.mitmproxy('conn.onCommandComplete(command="^PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;

-- we should be able to revocer the transaction and
-- see that the command is rollbacked
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

-- killing on command complete of COMMIT PREPARE, we should see that the command succeeds
-- and all the workers committed
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT PREPARED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');

-- we shouldn't have any prepared transactions in the workers
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- kill as soon as the coordinator sends COMMIT
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
-- Since we kill connections to one worker after commit arrives but the
-- other worker connections are healthy, we cannot commit on 1 worker
-- which has 2 active shard placements, but the other does. That's why
-- we expect to see 2 recovered prepared transactions.
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- finally, test failing on ROLLBACK with 2CPC
-- fail just after the coordinator sends the ROLLBACK
-- so the command can be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
TRUNCATE test_table;
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT count(*) FROM test_table;

-- but now kill just after the worker sends response to
-- ROLLBACK command, so we'll have lots of warnings but the command
-- should have been rollbacked both on the distributed table and the placements
SELECT citus.mitmproxy('conn.onCommandComplete(command="^ROLLBACK").kill()');
BEGIN;
TRUNCATE test_table;
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

-- final set of tests with 2PC and replication factor = 2
SET citus.shard_count = 4;
SET citus.shard_replication_factor = 2;

-- re-create the table with replication factor 2
DROP TABLE test_table CASCADE;
CREATE TABLE test_table (key int, value int);
SELECT create_distributed_table('test_table', 'key');
INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

CREATE VIEW unhealthy_shard_count AS
  SELECT count(*)
  FROM pg_dist_shard_placement pdsp
  JOIN
  pg_dist_shard pds
  ON pdsp.shardid=pds.shardid
  WHERE logicalrelid='truncate_failure.test_table'::regclass AND shardstate != 1;

-- in the first test, kill just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.onAuthenticationOk().cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="TRUNCATE TABLE truncate_failure.test_table").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends TRUNCATE TABLE command
SELECT citus.mitmproxy('conn.onQuery(query="TRUNCATE TABLE truncate_failure.test_table").cancel(' ||  pg_backend_pid() || ')');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- killing on PREPARE should be fine, everything should be rollbacked
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;

-- we should be able to revocer the transaction and
-- see that the command is rollbacked
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

-- killing on command complete of COMMIT PREPARE, we should see that the command succeeds
-- and all the workers committed
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT PREPARED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;

-- we shouldn't have any prepared transactions in the workers
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- kill as soon as the coordinator sends COMMIT
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").kill()');
TRUNCATE test_table;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
-- Since we kill connections to one worker after commit arrives but the
-- other worker connections are healthy, we cannot commit on 1 worker
-- which has 4 active shard placements (2 shards, replication factor=2),
-- but the other does. That's why we expect to see 4 recovered prepared
-- transactions.
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

INSERT INTO test_table SELECT x,x FROM generate_series(1,20) as f(x);

-- finally, test failing on ROLLBACK with 2CPC
-- fail just after the coordinator sends the ROLLBACK
-- so the command can be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
TRUNCATE test_table;
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- but now kill just after the worker sends response to
-- ROLLBACK command, so we'll have lots of warnings but the command
-- should have been rollbacked both on the distributed table and the placements
SELECT citus.mitmproxy('conn.onCommandComplete(command="^ROLLBACK").kill()');
BEGIN;
TRUNCATE test_table;
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT recover_prepared_transactions();
SELECT count(*) FROM test_table;

DROP SCHEMA truncate_failure CASCADE;
SET search_path TO default;
