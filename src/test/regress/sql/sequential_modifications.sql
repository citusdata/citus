-- 
-- Tests sequential and parallel DDL command execution
-- in combination with 1PC and 2PC
-- Note: this test should not be executed in parallel with
-- other tests since we're relying on disabling 2PC recovery 
--
CREATE SCHEMA test_seq_ddl;
SET search_path TO 'test_seq_ddl';

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

-- disbable 2PC recovery since our tests will check that
ALTER SYSTEM SET citus.recover_2pc_interval TO -1;
SELECT pg_reload_conf();

CREATE TABLE test_table(a int, b int);
SELECT create_distributed_table('test_table', 'a');

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

ALTER SYSTEM SET citus.recover_2pc_interval TO DEFAULT;
SET citus.shard_replication_factor TO DEFAULT;
SELECT pg_reload_conf();

SET search_path TO 'public';
DROP SCHEMA test_seq_ddl CASCADE;
