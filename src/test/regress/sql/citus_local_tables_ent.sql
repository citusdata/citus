\set VERBOSITY terse

SET citus.next_shard_id TO 1511000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_tables_ent;
SET search_path TO citus_local_tables_ent;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

CREATE TABLE citus_local_table (a int, b int);
SELECT citus_add_local_table_to_metadata('citus_local_table');

-- isolate_tenant_to_new_shard is not supported
SELECT isolate_tenant_to_new_shard('citus_local_table', 100, shard_transfer_mode => 'block_writes');

-- citus_copy_shard_placement is not supported
SELECT citus_copy_shard_placement(shardid, 'localhost', :master_port, 'localhost', :worker_1_port, false)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table'::regclass) as shardid;

-- citus_move_shard_placement is not supported
SELECT citus_move_shard_placement(shardid, 'localhost', :master_port, 'localhost', :worker_1_port)
FROM (SELECT shardid FROM pg_dist_shard WHERE logicalrelid='citus_local_table'::regclass) as shardid;

-- replicate_table_shards is not suported
SELECT replicate_table_shards('citus_local_table'::regclass, 2);

-- rebalance_table_shards is not supported
SELECT rebalance_table_shards('citus_local_table');

-- get_rebalance_table_shards_plan is not supported
SELECT get_rebalance_table_shards_plan('citus_local_table');

-- test a policy defined after creating a citus local table

-- create another user for policy test
CREATE USER user_can_select_a_1;
ALTER ROLE user_can_select_a_1 SET search_path TO citus_local_tables_ent;
GRANT USAGE ON SCHEMA citus_local_tables_ent TO user_can_select_a_1;

INSERT INTO citus_local_table VALUES (1,1);
INSERT INTO citus_local_table VALUES (2,2);

-- grant access
GRANT SELECT ON TABLE citus_local_table TO user_can_select_a_1;

-- enable row level security
ALTER TABLE citus_local_table ENABLE ROW LEVEL SECURITY;

-- switch user, it should not be able to see any rows since row level security is enabled
SET ROLE user_can_select_a_1;
SELECT * FROM citus_local_table ORDER BY 1, 2;
RESET ROLE;

-- create policy for user to read access for rows with a=1
CREATE POLICY user_mod ON citus_local_table
FOR SELECT
TO user_can_select_a_1
USING (current_user = 'user_can_select_a_1' and a=1);

-- switch user, it should be able to see rows with a=1
SET ROLE user_can_select_a_1;
SELECT * FROM citus_local_table ORDER BY 1, 2;

-- reset role
RESET ROLE;

-- cleanup at exit
DROP SCHEMA citus_local_tables_ent CASCADE;
