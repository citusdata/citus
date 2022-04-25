CREATE SCHEMA distribute_lock_tests;
SET search_path TO distribute_lock_tests;

SET citus.next_shard_id TO 10000;

CREATE TABLE dist_table(a int);

SELECT create_distributed_table('dist_table', 'a');

INSERT INTO dist_table SELECT n FROM generate_series(1, 5) n;

-- Test acquiring lock outside transaction
LOCK dist_table IN ACCESS EXCLUSIVE MODE;

-- Test acquiring lock inside procedure
DO $$
BEGIN
LOCK dist_table IN ACCESS EXCLUSIVE MODE;
END;
$$;

-- Test that when the user does not have the required permissions to lock
-- the locks are not forwarded to the workers
CREATE USER read_only_user;
GRANT SELECT ON distribute_lock_tests.dist_table TO read_only_user;
SET ROLE read_only_user;
SET citus.log_remote_commands TO ON;

BEGIN;
LOCK distribute_lock_tests.dist_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

RESET citus.log_local_commands;
RESET ROLE;

\c - - - :worker_1_port
SET search_path TO distribute_lock_tests;

-- Test trying to lock from a worker when the coordinator is not in the metadata
BEGIN;
LOCK dist_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Verify that the same restriction does not apply to worker local tables
CREATE TABLE local_table(a int);

-- Verify that no locks will be distributed for the local lock
SET citus.log_remote_commands TO ON;

BEGIN;
LOCK local_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

RESET citus.log_remote_commands;

-- Cleanup local table
DROP TABLE local_table;

SET citus.enable_manual_changes_to_shards TO OFF;

-- Test locking a shard
BEGIN;
LOCK dist_table_10000 IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Test allowing shard locks with the citus.enable_manual_changes_to_shards guc
SET citus.enable_manual_changes_to_shards TO ON;

BEGIN;
LOCK dist_table_10000 IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

RESET citus.enable_manual_changes_to_shards;

\c - - - :master_port
DROP SCHEMA distribute_lock_tests CASCADE;
DROP USER read_only_user;
