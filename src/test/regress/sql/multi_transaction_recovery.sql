ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1220000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1220000;

-- Tests for prepared transaction recovery

-- Ensure pg_dist_transaction is empty for test
SELECT recover_prepared_transactions();

SELECT * FROM pg_dist_transaction;

-- Create some "fake" prepared transactions to recover
\c - - - :worker_1_port

BEGIN;
CREATE TABLE should_abort (value int);
PREPARE TRANSACTION 'citus_0_should_abort';

BEGIN;
CREATE TABLE should_commit (value int);
PREPARE TRANSACTION 'citus_0_should_commit';

BEGIN;
CREATE TABLE should_be_sorted_into_middle (value int);
PREPARE TRANSACTION 'citus_0_should_be_sorted_into_middle';

\c - - - :master_port
-- Add "fake" pg_dist_transaction records and run recovery
INSERT INTO pg_dist_transaction VALUES (1, 'citus_0_should_commit');
INSERT INTO pg_dist_transaction VALUES (1, 'citus_0_should_be_forgotten');

SELECT recover_prepared_transactions();
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that transactions were correctly rolled forward
\c - - - :worker_1_port
SELECT count(*) FROM pg_tables WHERE tablename = 'should_abort';
SELECT count(*) FROM pg_tables WHERE tablename = 'should_commit';
