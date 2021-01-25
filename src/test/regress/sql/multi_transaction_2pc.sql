--
-- MULTI_TRANSACTION_2PC
--
-- Tests that check 2PC is used only for connections that perform a write
SET citus.next_shard_id TO 1410000;
SET citus.force_max_query_parallelization TO ON;
CREATE SCHEMA multi_transaction_2pc;
SET search_path = 'multi_transaction_2pc';
SET citus.shard_replication_factor to 1;

-- check that read-only participants skip prepare
-- only two of the connections will perform a write (INSERT)
CREATE TABLE test (a int);
SELECT create_distributed_table('test', 'a');
INSERT INTO test SELECT i FROM generate_series(0, 5)i;
SET citus.log_remote_commands TO ON;
BEGIN;
INSERT INTO test VALUES (1);
INSERT INTO test VALUES (2);
SELECT count(*) FROM test;
COMMIT;
SET citus.log_remote_commands TO OFF;

-- check that reads from a reference table don't trigger 2PC
-- despite repmodel being 2PC
CREATE TABLE test_reference (b int);
SELECT create_reference_table('test_reference');
SET citus.log_remote_commands TO ON;
INSERT INTO test_reference VALUES(1);
INSERT INTO test_reference VALUES(2);
BEGIN;
SELECT * FROM test_reference;
COMMIT;

SET citus.log_remote_commands TO OFF;
DROP SCHEMA multi_transaction_2pc CASCADE;
