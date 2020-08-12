CREATE SCHEMA mx_add_coordinator;
SET search_path TO mx_add_coordinator,public;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 7000000;
SET citus.next_placement_id TO 7000000;
SET citus.replication_model TO streaming;
SET client_min_messages TO WARNING;

CREATE USER reprefuser WITH LOGIN;
SELECT run_command_on_workers('CREATE USER reprefuser WITH LOGIN');
SET citus.enable_alter_role_propagation TO ON;
-- alter role for other than the extension owner works in enterprise, output differs accordingly
ALTER ROLE reprefuser WITH CREATEDB;

SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

-- test that coordinator pg_dist_node entry is synced to the workers
SELECT wait_until_metadata_sync(30000);

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

CREATE TABLE ref(groupid int);
SELECT create_reference_table('ref');

-- alter role from mx worker isn't propagated
\c - - - :worker_1_port
SET citus.enable_alter_role_propagation TO ON;
ALTER ROLE reprefuser WITH CREATEROLE;
select rolcreatedb, rolcreaterole from pg_roles where rolname = 'reprefuser';
\c - - - :worker_2_port
select rolcreatedb, rolcreaterole from pg_roles where rolname = 'reprefuser';
\c - - - :master_port
SET search_path TO mx_add_coordinator,public;
SET client_min_messages TO WARNING;
select rolcreatedb, rolcreaterole from pg_roles where rolname = 'reprefuser';

SET citus.log_local_commands TO ON;
SET client_min_messages TO DEBUG;

-- if the placement policy is not round-robin, SELECTs on the reference
-- tables use local execution
SELECT count(*) FROM ref;
SELECT count(*) FROM ref;

-- test that distributed functions also use local execution
CREATE OR REPLACE FUNCTION my_group_id()
RETURNS void
LANGUAGE plpgsql
SET search_path FROM CURRENT
AS $$
DECLARE
    gid int;
BEGIN
    SELECT groupid INTO gid
    FROM pg_dist_local_group;

    INSERT INTO mx_add_coordinator.ref(groupid) VALUES (gid);
END;
$$;
SELECT create_distributed_function('my_group_id()', colocate_with := 'ref');
SELECT my_group_id();
SELECT my_group_id();
SELECT DISTINCT(groupid) FROM ref ORDER BY 1;
TRUNCATE TABLE ref;
-- for round-robin policy, always go to workers
SET citus.task_assignment_policy TO "round-robin";
SELECT count(*) FROM ref;
SELECT count(*) FROM ref;
SELECT count(*) FROM ref;

SELECT my_group_id();
SELECT my_group_id();
SELECT my_group_id();
SELECT DISTINCT(groupid) FROM ref ORDER BY 1;
TRUNCATE TABLE ref;

-- modifications always go through local shard as well as remote ones
INSERT INTO ref VALUES (1);

-- get it ready for the next executions
TRUNCATE ref;
ALTER TABLE ref RENAME COLUMN groupid TO a;

-- test that changes from a metadata node is reflected in the coordinator placement
\c - - - :worker_1_port
SET search_path TO mx_add_coordinator,public;
INSERT INTO ref VALUES (1), (2), (3);
UPDATE ref SET a = a + 1;
DELETE FROM ref WHERE a > 3;

-- Test we allow reference/local joins on mx workers
CREATE TABLE local_table (a int);
INSERT INTO local_table VALUES (2), (4);

SELECT r.a FROM ref r JOIN local_table lt on r.a = lt.a;


\c - - - :master_port
SET search_path TO mx_add_coordinator,public;
SELECT * FROM ref ORDER BY a;

-- Clear pg_dist_transaction before removing the node. This is to keep the output
-- of multi_mx_transaction_recovery consistent.
SELECT recover_prepared_transactions();
SELECT count(*) FROM run_command_on_workers('SELECT recover_prepared_transactions()');

SELECT master_remove_node('localhost', :master_port);

-- test that coordinator pg_dist_node entry was removed from the workers
SELECT wait_until_metadata_sync(30000);
SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

SET client_min_messages TO error;
DROP SCHEMA mx_add_coordinator CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
