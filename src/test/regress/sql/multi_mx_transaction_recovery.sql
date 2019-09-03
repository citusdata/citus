-- Tests for running transaction recovery from a worker node

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.replication_model TO streaming;

CREATE TABLE test_recovery (x text);
SELECT create_distributed_table('test_recovery', 'x');

\c - - - :worker_1_port

-- Disable auto-recovery for the initial tests
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

SET citus.multi_shard_commit_protocol TO '2pc';

-- Ensure pg_dist_transaction is empty for test
SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- If the groupid of the worker changes this query will produce a
-- different result and the prepared statement names should be adapted
-- accordingly.
SELECT * FROM pg_dist_local_group;

BEGIN;
CREATE TABLE table_should_abort (value int);
PREPARE TRANSACTION 'citus_14_should_abort';

BEGIN;
CREATE TABLE table_should_commit (value int);
PREPARE TRANSACTION 'citus_14_should_commit';

BEGIN;
CREATE TABLE should_be_sorted_into_middle (value int);
PREPARE TRANSACTION 'citus_14_should_be_sorted_into_middle';

-- this node (e.g., node id 14) should not touch
-- transactions with different nodeIds in the gid
BEGIN;
CREATE TABLE table_should_do_nothing (value int);
PREPARE TRANSACTION 'citus_122_should_do_nothing';

-- Add "fake" pg_dist_transaction records and run recovery
INSERT INTO pg_dist_transaction VALUES (14, 'citus_14_should_commit');
INSERT INTO pg_dist_transaction VALUES (14, 'citus_14_should_be_forgotten');
INSERT INTO pg_dist_transaction VALUES (122, 'citus_122_should_do_nothing');

SELECT recover_prepared_transactions();

-- delete the citus_122_should_do_nothing transaction
DELETE FROM pg_dist_transaction WHERE gid = 'citus_122_should_do_nothing' RETURNING *;
ROLLBACK PREPARED 'citus_122_should_do_nothing';

SELECT count(*) FROM pg_dist_transaction;

SELECT count(*) FROM pg_tables WHERE tablename = 'table_should_abort';
SELECT count(*) FROM pg_tables WHERE tablename = 'table_should_commit';

-- plain INSERT does not use 2PC
INSERT INTO test_recovery VALUES ('hello');
SELECT count(*) FROM pg_dist_transaction;

-- Multi-statement transactions should write 2 transaction recovery records
BEGIN;
SET LOCAL citus.enable_local_execution TO false;
INSERT INTO test_recovery VALUES ('hello');
INSERT INTO test_recovery VALUES ('world');
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- the same transaction block, but this time
-- enable local execution as well. The first
-- command is locally executed, the second
-- is remote, so 1 entry is expected
BEGIN;
INSERT INTO test_recovery VALUES ('hello');
INSERT INTO test_recovery VALUES ('world');
COMMIT;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- Committed INSERT..SELECT via coordinator should write 4 transaction recovery records
INSERT INTO test_recovery (x) SELECT 'hello-'||s FROM generate_series(1,100) s;
SELECT count(*) FROM pg_dist_transaction;
SELECT recover_prepared_transactions();

-- Committed COPY should write 3 transaction records (2 fall into the same shard)
COPY test_recovery (x) FROM STDIN CSV;
hello-0
hello-1
world-0
world-1
\.

SELECT count(*) FROM pg_dist_transaction;

-- Test whether auto-recovery runs
ALTER SYSTEM SET citus.recover_2pc_interval TO 10;
SELECT pg_reload_conf();
-- Sleep 1 second to give Valgrind enough time to clear transactions
SELECT pg_sleep(1);
SELECT count(*) FROM pg_dist_transaction;
ALTER SYSTEM RESET citus.recover_2pc_interval;
SELECT pg_reload_conf();

DROP TABLE table_should_commit;

\c - - - :master_port

DROP TABLE test_recovery_ref;
DROP TABLE test_recovery;
