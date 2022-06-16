CREATE SCHEMA mx_add_coordinator;
SET search_path TO mx_add_coordinator,public;
SET client_min_messages TO WARNING;

CREATE USER reprefuser WITH LOGIN;
SET citus.enable_alter_role_propagation TO ON;
-- alter role for other than the extension owner works in enterprise, output differs accordingly
ALTER ROLE reprefuser WITH CREATEDB;

-- check connectivity in the cluster
-- verify that we test for 4 node pairs before we add coordinator to metadata
SELECT bool_and(coalesce(result, false)), count(*) FROM citus_check_cluster_node_health();

SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);

-- verify that we test for 9 node pairs after we add coordinator to metadata
SELECT bool_and(coalesce(result, false)), count(*) FROM citus_check_cluster_node_health();

-- test that we can test for connectivity when connected to worker nodes as well
\c - - - :worker_1_port
SELECT bool_and(coalesce(result, false)), count(*) FROM citus_check_cluster_node_health();

\c - - - :master_port

-- set the configs after reconnecting to coordinator
SET search_path TO mx_add_coordinator,public;
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 8;
SET citus.next_shard_id TO 7000000;
SET citus.next_placement_id TO 7000000;
SET client_min_messages TO WARNING;

-- test that metadata is synced to the workers
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

CREATE TABLE ref(groupid int);
SELECT create_reference_table('ref');

\c - - - :worker_1_port
-- alter role from mx worker isn't allowed when alter role propagation is on
SET citus.enable_alter_role_propagation TO ON;
ALTER ROLE reprefuser WITH CREATEROLE;
-- to alter role locally disable alter role propagation first
SET citus.enable_alter_role_propagation TO OFF;
ALTER ROLE reprefuser WITH CREATEROLE;
SELECT rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = 'reprefuser';
RESET citus.enable_alter_role_propagation;

\c - - - :worker_2_port
-- show that altering role locally on worker doesn't propagated to other worker
SELECT rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = 'reprefuser';

\c - - - :master_port
SET search_path TO mx_add_coordinator,public;
-- show that altering role locally on worker doesn't propagated to coordinator
SELECT rolcreatedb, rolcreaterole FROM pg_roles WHERE rolname = 'reprefuser';

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
SELECT stop_metadata_sync_to_node('localhost', :master_port);

SELECT * FROM ref ORDER BY a;

-- Clear pg_dist_transaction before removing the node. This is to keep the output
-- of multi_mx_transaction_recovery consistent.
SELECT recover_prepared_transactions();
SELECT count(*) FROM run_command_on_workers('SELECT recover_prepared_transactions()');

-- make sure that we can change a column type
-- that appears in foreign key to reference table
SET citus.shard_replication_factor TO 1;
CREATE TABLE local_fkey_table(id int PRIMARY KEY);
CREATE TABLE referece_table(id int PRIMARY KEY);
SELECT create_reference_table('referece_table');

CREATE TABLE distributed_table(id int PRIMARY KEY, value_1 int);
SELECT create_distributed_table('distributed_table', 'id');
INSERT INTO local_fkey_table SELECT i FROM generate_Series(0,100)i;
INSERT INTO referece_table SELECT i FROM generate_Series(0,100)i;
INSERT INTO distributed_table SELECT i, i FROM generate_Series(0,100)i;

ALTER TABLE
	distributed_table
ADD CONSTRAINT
	fkey_delete FOREIGN KEY(value_1)
REFERENCES
	referece_table(id) ON DELETE CASCADE;

ALTER TABLE
	referece_table
ADD CONSTRAINT
	fkey_delete_2 FOREIGN KEY(id)
REFERENCES
	local_fkey_table(id);

ALTER TABLE
	local_fkey_table
ADD CONSTRAINT
	fkey_delete_3 FOREIGN KEY(id)
REFERENCES
	referece_table(id);

-- now we've a distributed table which has a foreign key to a reference table
-- note that this only happens in MX
alter table distributed_table ALTER COLUMN value_1 TYPE bigint;
alter table distributed_table ALTER COLUMN value_1 TYPE int;
alter table referece_table ALTER COLUMN id TYPE bigint;
alter table referece_table ALTER COLUMN id TYPE int;
alter table local_fkey_table ALTER COLUMN id TYPE int;

SET citus.force_max_query_parallelization TO ON;
alter table distributed_table ALTER COLUMN value_1 TYPE bigint;
alter table distributed_table ALTER COLUMN value_1 TYPE int;

SET client_min_messages TO ERROR;
DROP TABLE distributed_table, referece_table, local_fkey_table;
SELECT master_remove_node('localhost', :master_port);

-- test that metadata is synced to the workers
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT 1 FROM citus_activate_node('localhost', :worker_2_port);

SELECT verify_metadata('localhost', :worker_1_port),
       verify_metadata('localhost', :worker_2_port);

SET client_min_messages TO error;
DROP SCHEMA mx_add_coordinator CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
