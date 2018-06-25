-- 
-- Tests sequential and parallel DDL command execution
-- in combination with 1PC and 2PC
-- Note: this test should not be executed in parallel with
-- other tests since we're relying on disabling 2PC recovery 
--
CREATE SCHEMA test_seq_ddl;
SET search_path TO 'test_seq_ddl';
SET citus.next_shard_id TO 16000;
SET citus.next_placement_id TO 16000;

-- this function simply checks the equality of the number of transactions in the
-- pg_dist_transaction and number of primary worker nodes
-- The function is useful to ensure that a single connection is opened per worker
-- in a distributed transaction
CREATE OR REPLACE FUNCTION distributed_2PCs_are_equal_to_worker_count()
    RETURNS bool AS
$$
DECLARE
    result bool;
BEGIN
    SELECT tx_count = worker_count FROM (SELECT count(*) as  tx_count FROM pg_dist_transaction WHERE gid LIKE 'citus_%_' || pg_backend_pid() || '%_%') as s1,  (SELECT count(*) as worker_count FROM pg_dist_node WHERE noderole = 'primary') as s2 INTO result;
    RETURN result;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;


-- this function simply checks the equality of the number of transactions in the
-- pg_dist_transaction and number of shard placements for a distributed table
-- The function is useful to ensure that a single connection is opened per 
-- shard placement in a distributed transaction
CREATE OR REPLACE FUNCTION distributed_2PCs_are_equal_to_placement_count()
    RETURNS bool AS
$$
DECLARE
    result bool;
BEGIN
    SELECT count(*) = current_setting('citus.shard_count')::bigint * current_setting('citus.shard_replication_factor')::bigint FROM pg_dist_transaction WHERE gid LIKE 'citus_%_' || pg_backend_pid() || '%_%'
     INTO result;
    RETURN result;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;


-- this function simply checks existence of distributed transcations in
-- pg_dist_transaction
CREATE OR REPLACE FUNCTION no_distributed_2PCs()
    RETURNS bool AS
$$
DECLARE
    result bool;
BEGIN
    SELECT tx_count = 0 FROM (SELECT count(*) as  tx_count FROM pg_dist_transaction WHERE gid LIKE 'citus_%_' || pg_backend_pid() || '%_%') as s1
     INTO result;
    RETURN result;
END;
$$
LANGUAGE 'plpgsql' IMMUTABLE;

CREATE OR REPLACE FUNCTION set_local_multi_shard_modify_mode_to_sequential()
    RETURNS void
    LANGUAGE C STABLE STRICT
    AS 'citus', $$set_local_multi_shard_modify_mode_to_sequential$$;

-- disbable 2PC recovery since our tests will check that
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

CREATE TABLE test_table(a int, b int);
SELECT create_distributed_table('test_table', 'a');

-- not useful if not in transaction
SELECT set_local_multi_shard_modify_mode_to_sequential();

-- we should see #worker transactions
-- when sequential mode is used
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
ALTER TABLE test_table ADD CONSTRAINT a_check CHECK(a > 0);
SELECT distributed_2PCs_are_equal_to_worker_count();

-- we should see placement count # transactions
-- when parallel mode is used
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
ALTER TABLE test_table ADD CONSTRAINT b_check CHECK(b > 0);
SELECT distributed_2PCs_are_equal_to_placement_count();

-- with 1PC, we should not see and distributed TXs in the pg_dist_transaction
SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
ALTER TABLE test_table ADD CONSTRAINT c_check CHECK(a > 0);
SELECT no_distributed_2PCs();

SET citus.multi_shard_commit_protocol TO '1pc';
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
ALTER TABLE test_table ADD CONSTRAINT d_check CHECK(a > 0);
SELECT no_distributed_2PCs();

CREATE TABLE ref_test(a int);
SELECT create_reference_table('ref_test');
SET citus.multi_shard_commit_protocol TO '1pc';

-- reference tables should always use 2PC
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
CREATE INDEX ref_test_seq_index ON ref_test(a);
SELECT distributed_2PCs_are_equal_to_worker_count();

-- reference tables should always use 2PC
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
CREATE INDEX ref_test_seq_index_2 ON ref_test(a);
SELECT distributed_2PCs_are_equal_to_worker_count();

-- tables with replication factor > 1 should also obey 
-- both multi_shard_commit_protocol and multi_shard_modify_mode
SET citus.shard_replication_factor TO 2;
CREATE TABLE test_table_rep_2 (a int);
SELECT create_distributed_table('test_table_rep_2', 'a');
 
-- 1PC should never use 2PC with rep > 1
SET citus.multi_shard_commit_protocol TO '1pc';

SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
CREATE INDEX test_table_rep_2_i_1 ON test_table_rep_2(a);
SELECT no_distributed_2PCs();

SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
CREATE INDEX test_table_rep_2_i_2 ON test_table_rep_2(a);
SELECT no_distributed_2PCs();

-- 2PC should always use 2PC with rep > 1
SET citus.multi_shard_commit_protocol TO '2pc';
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
CREATE INDEX test_table_rep_2_i_3 ON test_table_rep_2(a);
SELECT distributed_2PCs_are_equal_to_worker_count();

SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
CREATE INDEX test_table_rep_2_i_4 ON test_table_rep_2(a);
SELECT distributed_2PCs_are_equal_to_placement_count();

-- CREATE INDEX CONCURRENTLY should work fine with rep > 1
-- with both 2PC and different parallel modes
SET citus.multi_shard_commit_protocol TO '2pc';
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
CREATE INDEX CONCURRENTLY test_table_rep_2_i_5 ON test_table_rep_2(a);

-- we shouldn't see any distributed transactions
SELECT no_distributed_2PCs();

SET citus.multi_shard_commit_protocol TO '2pc';
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
CREATE INDEX CONCURRENTLY test_table_rep_2_i_6 ON test_table_rep_2(a);
-- we shouldn't see any distributed transactions
SELECT no_distributed_2PCs();

-- test TRUNCATE on sequential and parallel modes
CREATE TABLE test_seq_truncate (a int);
INSERT INTO test_seq_truncate SELECT i FROM generate_series(0, 100) i;
SELECT create_distributed_table('test_seq_truncate', 'a');

-- with parallel modification mode, we should see #shards records
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
TRUNCATE test_seq_truncate;
SELECT distributed_2PCs_are_equal_to_placement_count();

-- with sequential modification mode, we should see #primary worker records
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
TRUNCATE test_seq_truncate;
SELECT distributed_2PCs_are_equal_to_worker_count();

-- truncate with rep > 1 should work both in parallel and seq. modes
CREATE TABLE test_seq_truncate_rep_2 (a int);
SET citus.shard_replication_factor TO 2;
SELECT create_distributed_table('test_seq_truncate_rep_2', 'a');
INSERT INTO test_seq_truncate_rep_2 SELECT i FROM generate_series(0, 100) i;

SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
TRUNCATE test_seq_truncate_rep_2;
SELECT distributed_2PCs_are_equal_to_worker_count();

SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
TRUNCATE test_seq_truncate_rep_2;
SELECT distributed_2PCs_are_equal_to_placement_count();

CREATE TABLE multi_shard_modify_test (
        t_key integer not null,
        t_name varchar(25) not null,
        t_value integer not null);
SELECT create_distributed_table('multi_shard_modify_test', 't_key');

-- with parallel modification mode, we should see #shards records
SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test');
SELECT distributed_2PCs_are_equal_to_placement_count();

-- with sequential modification mode, we should see #primary worker records
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
SELECT master_modify_multiple_shards('DELETE FROM multi_shard_modify_test');
SELECT distributed_2PCs_are_equal_to_worker_count();

-- one more realistic test with sequential inserts and truncate in the same tx
INSERT INTO multi_shard_modify_test SELECT i, i::text, i FROM generate_series(0,100) i;
BEGIN;
    INSERT INTO multi_shard_modify_test VALUES (1,'1',1), (2,'2',2), (3,'3',3), (4,'4',4);

    -- now switch to sequential mode to enable a successful TRUNCATE
    SELECT set_local_multi_shard_modify_mode_to_sequential();
    TRUNCATE multi_shard_modify_test;
COMMIT;

-- see that all the data successfully removed
SELECT count(*) FROM multi_shard_modify_test;

-- test INSERT ... SELECT queries
-- with sequential modification mode, we should see #primary worker records
SET citus.multi_shard_modify_mode TO 'sequential';
SELECT recover_prepared_transactions();
INSERT INTO multi_shard_modify_test SELECT * FROM multi_shard_modify_test;
SELECT distributed_2PCs_are_equal_to_worker_count();

SET citus.multi_shard_modify_mode TO 'parallel';
SELECT recover_prepared_transactions();
INSERT INTO multi_shard_modify_test SELECT * FROM multi_shard_modify_test;
SELECT distributed_2PCs_are_equal_to_placement_count();

-- one more realistic test with sequential inserts and INSERT .. SELECT in the same tx
INSERT INTO multi_shard_modify_test SELECT i, i::text, i FROM generate_series(0,100) i;
BEGIN;
    INSERT INTO multi_shard_modify_test VALUES (1,'1',1), (2,'2',2), (3,'3',3), (4,'4',4);

    -- now switch to sequential mode to enable a successful INSERT .. SELECT
    SELECT set_local_multi_shard_modify_mode_to_sequential();
    INSERT INTO multi_shard_modify_test SELECT * FROM multi_shard_modify_test;
COMMIT;

-- see that all the data successfully inserted
SELECT count(*) FROM multi_shard_modify_test;

ALTER SYSTEM SET citus.recover_2pc_interval TO DEFAULT;
SET citus.shard_replication_factor TO DEFAULT;
SELECT pg_reload_conf();

-- The following tests are added to test if create_distributed_table honors sequential mode
SELECT recover_prepared_transactions();

-- Check if multi_shard_update works properly after create_distributed_table in sequential mode
CREATE TABLE test_seq_multi_shard_update(a int, b int);
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT create_distributed_table('test_seq_multi_shard_update', 'a');
    INSERT INTO test_seq_multi_shard_update VALUES (0, 0), (1, 0), (2, 0), (3, 0), (4, 0);
    SELECT master_modify_multiple_shards('DELETE FROM test_seq_multi_shard_update WHERE b < 2');
COMMIT;
SELECT distributed_2PCs_are_equal_to_worker_count();
DROP TABLE test_seq_multi_shard_update;


-- Check if truncate works properly after create_distributed_table in sequential mode
SELECT recover_prepared_transactions();
CREATE TABLE test_seq_truncate_after_create(a int, b int);
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT create_distributed_table('test_seq_truncate_after_create', 'a');
    INSERT INTO test_seq_truncate_after_create VALUES (0, 0), (1, 0), (2, 0), (3, 0), (4, 0);
    TRUNCATE test_seq_truncate_after_create;
COMMIT;
SELECT distributed_2PCs_are_equal_to_worker_count();
DROP TABLE test_seq_truncate_after_create;


-- Check if drop table works properly after create_distributed_table in sequential mode
SELECT recover_prepared_transactions();
CREATE TABLE test_seq_drop_table(a int, b int);
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT create_distributed_table('test_seq_drop_table', 'a');
    DROP TABLE test_seq_drop_table;
COMMIT;
SELECT distributed_2PCs_are_equal_to_worker_count();


-- Check if copy errors out properly after create_distributed_table in sequential mode
SELECT recover_prepared_transactions();
CREATE TABLE test_seq_copy(a int, b int);
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT create_distributed_table('test_seq_copy', 'a');
    \COPY test_seq_copy FROM STDIN DELIMITER AS ',';
1,1
2,2
3,3
\.
ROLLBACK;
SELECT distributed_2PCs_are_equal_to_worker_count();
DROP TABLE test_seq_copy;


-- Check if DDL + CREATE INDEX works properly after create_distributed_table in sequential mode
SELECT recover_prepared_transactions();
CREATE TABLE test_seq_ddl_index(a int, b int);
BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    SELECT create_distributed_table('test_seq_ddl_index', 'a');
    INSERT INTO test_seq_ddl_index VALUES (0, 0), (1, 0), (2, 0), (3, 0), (4, 0);
    ALTER TABLE test_seq_ddl_index ADD COLUMN c int;
    CREATE INDEX idx ON test_seq_ddl_index(c);
COMMIT;
SELECT distributed_2PCs_are_equal_to_worker_count();
DROP TABLE test_seq_ddl_index;

-- create_distributed_table should fail on relations with data in sequential mode in and out transaction block
CREATE TABLE test_create_seq_table (a int);
INSERT INTO test_create_seq_table VALUES (1);

SET citus.multi_shard_modify_mode TO 'sequential';
SELECT create_distributed_table('test_create_seq_table' ,'a');

RESET citus.multi_shard_modify_mode;

BEGIN;
    SET LOCAL citus.multi_shard_modify_mode TO 'sequential';
    select create_distributed_table('test_create_seq_table' ,'a');
ROLLBACK;

SET search_path TO 'public';
DROP SCHEMA test_seq_ddl CASCADE;
