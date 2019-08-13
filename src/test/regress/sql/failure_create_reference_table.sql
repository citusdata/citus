--
-- Failure tests for creating reference table
--

CREATE SCHEMA failure_reference_table;
SET search_path TO 'failure_reference_table';

SET citus.next_shard_id TO 10000000;

SELECT citus.mitmproxy('conn.allow()');

CREATE TABLE ref_table(id int);
INSERT INTO ref_table VALUES(1),(2),(3);

-- Kill on sending first query to worker node, should error
-- out and not create any placement
SELECT citus.mitmproxy('conn.onQuery().kill()');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Kill after creating transaction on worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="BEGIN").kill()');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Cancel after creating transaction on worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="BEGIN").cancel(' || pg_backend_pid() || ')');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Kill after copying data to worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="SELECT 1").kill()');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Cancel after copying data to worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="SELECT 1").cancel(' || pg_backend_pid() || ')');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Kill after copying data to worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY 3").kill()');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- Cancel after copying data to worker node
SELECT citus.mitmproxy('conn.onCommandComplete(command="COPY 3").cancel(' || pg_backend_pid() || ')');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;

-- we don't want to see the prepared transaction numbers in the warnings
SET client_min_messages TO ERROR;

-- Kill after preparing transaction. Since we don't commit after preparing, we recover
-- prepared transaction afterwards.
SELECT citus.mitmproxy('conn.onCommandComplete(command="PREPARE TRANSACTION").kill()');
SELECT create_reference_table('ref_table');

SELECT count(*) FROM pg_dist_shard_placement;
SELECT recover_prepared_transactions();

-- Kill after commiting prepared, this should succeed
SELECT citus.mitmproxy('conn.onCommandComplete(command="COMMIT PREPARED").kill()');
SELECT create_reference_table('ref_table');

SELECT shardid, nodeport, shardstate FROM pg_dist_shard_placement ORDER BY shardid, nodeport;
SET client_min_messages TO NOTICE;

SELECT citus.mitmproxy('conn.allow()');
DROP TABLE ref_table;
DROP SCHEMA failure_reference_table;
CREATE SCHEMA failure_reference_table;
CREATE TABLE ref_table(id int);
INSERT INTO ref_table VALUES(1),(2),(3);

-- Test in transaction
SELECT citus.mitmproxy('conn.onQuery().kill()');
BEGIN;
SELECT create_reference_table('ref_table');
COMMIT;

-- kill on ROLLBACK, should be rollbacked
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").kill()');
BEGIN;
SELECT create_reference_table('ref_table');
ROLLBACK;

SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodeport;

-- cancel when the coordinator send ROLLBACK, should be rollbacked. We ignore cancellations
-- during the ROLLBACK.
SELECT citus.mitmproxy('conn.onQuery(query="^ROLLBACK").cancel(' || pg_backend_pid() || ')');
BEGIN;
SELECT create_reference_table('ref_table');
ROLLBACK;

SELECT * FROM pg_dist_shard_placement ORDER BY shardid, nodeport;

DROP SCHEMA failure_reference_table CASCADE;
SET search_path TO default;
