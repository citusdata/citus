--
--  Failure tests for COPY to reference tables
--
CREATE SCHEMA copy_reference_failure;
SET search_path TO 'copy_reference_failure';
SET citus.next_shard_id TO 130000;

-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO ERROR;

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE test_table(id int, value_1 int);
SELECT create_reference_table('test_table');

CREATE VIEW unhealthy_shard_count AS
  SELECT count(*)
  FROM pg_dist_shard_placement pdsp
  JOIN
  pg_dist_shard pds
  ON pdsp.shardid=pds.shardid
  WHERE logicalrelid='copy_reference_failure.test_table'::regclass AND shardstate != 1;

-- in the first test, kill just in the first
-- response we get from the worker
SELECT citus.mitmproxy('conn.kill()');
\copy test_table FROM STDIN DELIMITER ','
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
\copy test_table FROM STDIN DELIMITER ','
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends begin
SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").cancel(' ||  pg_backend_pid() || ')');
\copy test_table FROM STDIN DELIMITER ','
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the coordinator sends COPY command
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").kill()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends COPY command
SELECT citus.mitmproxy('conn.onQuery(query="^COPY").cancel(' ||  pg_backend_pid() || ')');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill as soon as the worker sends CopyComplete
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COPY 3").kill()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancel as soon as the coordinator sends CopyData
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COPY 3").cancel(' ||  pg_backend_pid() || ')');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- kill the connection when we try to start the COPY
-- the query should abort
SELECT citus.mitmproxy('conn.onQuery(query="FROM STDIN WITH").killall()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- killing on PREPARE should be fine, everything should be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").kill()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- cancelling on PREPARE should be fine, everything should be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").cancel(' ||  pg_backend_pid() || ')');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- killing on command complete of COMMIT PREPARE, we should see that the command succeeds
-- and all the workers committed
SELECT citus.mitmproxy('conn.onCommandComplete(command="^COMMIT PREPARED").kill()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');

-- we shouldn't have any prepared transactions in the workers
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

TRUNCATE test_table;

-- kill as soon as the coordinator sends COMMIT
SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT").kill()');
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
SELECT citus.mitmproxy('conn.allow()');
-- Since we kill connections to one worker after commit arrives but the
-- other worker connections are healthy, we cannot commit on 1 worker
-- which has 1 active shard placements, but the other does. That's why
-- we expect to see 1 recovered prepared transactions.
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;
TRUNCATE test_table;

-- finally, test failing on ROLLBACK just after the coordinator
-- sends the ROLLBACK so the command can be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
SET LOCAL client_min_messages TO WARNING;
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

-- but now kill just after the worker sends response to
-- ROLLBACK command, command should have been rollbacked
-- both on the distributed table and the placements
SELECT citus.mitmproxy('conn.onCommandComplete(command="^ROLLBACK").kill()');
BEGIN;
SET LOCAL client_min_messages TO WARNING;
\copy test_table FROM STDIN DELIMITER ','
1,2
2,3
3,4
\.
ROLLBACK;
SELECT citus.mitmproxy('conn.allow()');
SELECT recover_prepared_transactions();
SELECT * FROM unhealthy_shard_count;
SELECT count(*) FROM test_table;

DROP SCHEMA copy_reference_failure CASCADE;
SET search_path TO default;

