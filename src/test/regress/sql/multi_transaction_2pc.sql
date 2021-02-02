--
-- MULTI_TRANSACTION_2PC
--
-- Tests that check 2PC is used only for connections that perform a write
SET citus.next_shard_id TO 1410000;
SET citus.force_max_query_parallelization TO ON;
CREATE SCHEMA multi_transaction_2pc;
SET search_path = 'multi_transaction_2pc';
SET citus.shard_replication_factor TO 1;

-- check that read-only participants skip prepare
-- only two of the connections will perform a write (INSERT)
CREATE TABLE test (a int);
SELECT create_distributed_table('test', 'a');
INSERT INTO test SELECT i FROM generate_series(0, 5)i;
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
-- these inserts use two connections
INSERT INTO test VALUES (6);
INSERT INTO test VALUES (7);
SET LOCAL citus.log_remote_commands TO OFF;
-- we know this will use more than two connections
SELECT count(*) FROM test;
SET LOCAL citus.log_remote_commands TO ON;
COMMIT;

-- check that read-only participants skip prepare
-- only two of the connections will perform a write (INSERT)
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
-- this insert uses two connections
INSERT INTO test SELECT i FROM generate_series(8, 10)i;
SET LOCAL citus.log_remote_commands TO OFF;
-- we know this will use more than two connections
SELECT COUNT(*) FROM test;
SET LOCAL citus.log_remote_commands TO ON;
COMMIT;

-- check that reads from a reference table don't trigger 2PC
-- despite repmodel being 2PC
CREATE TABLE test_reference (b int);
SELECT create_reference_table('test_reference');
INSERT INTO test_reference VALUES(1);
INSERT INTO test_reference VALUES(2);
BEGIN;
SET LOCAL citus.log_remote_commands TO ON;
SELECT * FROM test_reference;
COMMIT;

DROP SCHEMA multi_transaction_2pc CASCADE;
