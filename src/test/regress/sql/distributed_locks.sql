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

-- Try all valid lock options; also try omitting the optional TABLE keyword.
BEGIN TRANSACTION;
LOCK TABLE dist_table IN ACCESS SHARE MODE;
LOCK dist_table IN ROW SHARE MODE;
LOCK TABLE dist_table IN ROW EXCLUSIVE MODE;
LOCK TABLE dist_table IN SHARE UPDATE EXCLUSIVE MODE;
LOCK TABLE dist_table IN SHARE MODE;
LOCK dist_table IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE dist_table IN EXCLUSIVE MODE;
LOCK TABLE dist_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Test that when the user does not have the required permissions to lock
-- the locks are not forwarded to the workers

SET client_min_messages TO ERROR;
SELECT run_command_on_workers($$
    SET citus.enable_ddl_propagation TO OFF;
    CREATE ROLE read_only_user WITH LOGIN;
    RESET citus.enable_ddl_propagation;
$$);

SET citus.enable_ddl_propagation TO OFF;
CREATE ROLE read_only_user WITH LOGIN;
GRANT ALL ON SCHEMA distribute_lock_tests TO read_only_user;
GRANT SELECT ON dist_table TO read_only_user;
RESET citus.enable_ddl_propagation;
RESET client_min_messages;

SET ROLE read_only_user;
SET citus.log_remote_commands TO ON;

BEGIN;
LOCK dist_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

SET citus.log_remote_commands TO OFF;
RESET ROLE;

-- test that user with view permissions can lock the tables
-- which the view is built on
CREATE VIEW myview AS SELECT * FROM dist_table;

SET client_min_messages TO ERROR;
SELECT run_command_on_workers($$
    SET citus.enable_ddl_propagation TO OFF;
    CREATE ROLE user_with_view_permissions WITH LOGIN;
    GRANT ALL ON SCHEMA distribute_lock_tests TO user_with_view_permissions;
    GRANT ALL ON distribute_lock_tests.myview TO user_with_view_permissions;
    RESET citus.enable_ddl_propagation;
$$);

SET citus.enable_ddl_propagation TO OFF;
CREATE ROLE user_with_view_permissions WITH LOGIN;
GRANT ALL ON SCHEMA distribute_lock_tests TO user_with_view_permissions;
GRANT ALL ON myview TO user_with_view_permissions;
RESET citus.enable_ddl_propagation;
RESET client_min_messages;

SET ROLE TO user_with_view_permissions;

BEGIN;
LOCK myview IN ACCESS EXCLUSIVE MODE;
SELECT run_command_on_workers($$
    SELECT mode FROM pg_locks WHERE relation = 'distribute_lock_tests.dist_table'::regclass ORDER BY 1;
$$);

ROLLBACK;

RESET ROLE;

\c - - - :worker_1_port
SET search_path TO distribute_lock_tests;

-- Test trying to lock from a worker when the coordinator is not in the metadata
SET citus.allow_unsafe_locks_from_workers TO 'off';
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

-- Test that setting the guc to 'on' will allow the lock from workers
SET citus.allow_unsafe_locks_from_workers TO 'on';
BEGIN;
LOCK dist_table IN ACCESS EXCLUSIVE MODE;
ROLLBACK;

-- Test locking a shard
SET citus.enable_manual_changes_to_shards TO OFF;
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
SET citus.enable_ddl_propagation TO OFF;
DROP ROLE read_only_user;
DROP ROLE user_with_view_permissions;
RESET citus.enable_ddl_propagation;
SELECT run_command_on_workers($$
    SET citus.enable_ddl_propagation TO OFF;
    DROP USER read_only_user;
    DROP USER user_with_view_permissions;
    RESET citus.enable_ddl_propagation;
$$);
