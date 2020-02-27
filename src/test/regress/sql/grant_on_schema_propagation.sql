--
-- GRANT_ON_SCHEMA_PROPAGATION
--

-- test grants are propagated when the schema is
CREATE SCHEMA dist_schema;
CREATE TABLE dist_schema.dist_table (id int);
CREATE SCHEMA another_dist_schema;
CREATE TABLE another_dist_schema.dist_table (id int);
CREATE SCHEMA non_dist_schema;

-- create roles on all nodes
SELECT run_command_on_coordinator_and_workers('CREATE USER role_1');
SELECT run_command_on_coordinator_and_workers('CREATE USER role_2');
SELECT run_command_on_coordinator_and_workers('CREATE USER role_3');

-- do some varying grants
GRANT USAGE, CREATE ON SCHEMA dist_schema TO role_1 WITH GRANT OPTION;
GRANT USAGE ON SCHEMA dist_schema TO role_2;
SET ROLE role_1;
GRANT USAGE ON SCHEMA dist_schema TO role_3 WITH GRANT OPTION;
GRANT CREATE ON SCHEMA dist_schema TO role_3;
GRANT CREATE, USAGE ON SCHEMA dist_schema TO PUBLIC;
RESET ROLE;
GRANT USAGE ON SCHEMA dist_schema TO PUBLIC;

SELECT create_distributed_table('dist_schema.dist_table', 'id');
SELECT create_distributed_table('another_dist_schema.dist_table', 'id');

SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'dist_schema';
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'dist_schema';
\c - - - :master_port

-- grant all permissions
GRANT ALL ON SCHEMA dist_schema, another_dist_schema, non_dist_schema TO role_1, role_2, role_3 WITH GRANT OPTION;
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- revoke all permissions
REVOKE ALL ON SCHEMA dist_schema, another_dist_schema, non_dist_schema FROM role_1, role_2, role_3, PUBLIC CASCADE;
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- grant with multiple permissions, roles and schemas
GRANT USAGE, CREATE ON SCHEMA dist_schema, another_dist_schema, non_dist_schema TO role_1, role_2, role_3;
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- revoke with multiple permissions, roles and schemas
REVOKE USAGE, CREATE ON SCHEMA dist_schema, another_dist_schema, non_dist_schema FROM role_1, role_2;
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- grant with grant option
GRANT USAGE ON SCHEMA dist_schema TO role_1, role_3 WITH GRANT OPTION;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- revoke grant option for
REVOKE GRANT OPTION FOR USAGE ON SCHEMA dist_schema FROM role_3;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port

-- test current_user
SET citus.enable_alter_role_propagation TO ON;
ALTER ROLE role_1 SUPERUSER;
SET citus.enable_alter_role_propagation TO OFF;
SET ROLE role_1;
GRANT CREATE ON SCHEMA dist_schema TO CURRENT_USER;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname IN ('dist_schema', 'another_dist_schema', 'non_dist_schema') ORDER BY nspname;
\c - - - :master_port
RESET ROLE;
SET citus.enable_alter_role_propagation TO ON;
ALTER ROLE role_1 NOSUPERUSER;
SET citus.enable_alter_role_propagation TO OFF;

DROP TABLE dist_schema.dist_table, another_dist_schema.dist_table;
SELECT run_command_on_coordinator_and_workers('DROP SCHEMA dist_schema');
SELECT run_command_on_coordinator_and_workers('DROP SCHEMA another_dist_schema');
SELECT run_command_on_coordinator_and_workers('DROP SCHEMA non_dist_schema');

-- test if the grantors are propagated correctly
-- first remove one of the worker nodes
SET citus.shard_replication_factor TO 1;
SELECT master_remove_node(:'worker_2_host', :worker_2_port);

-- create a new schema
CREATE SCHEMA grantor_schema;

-- give cascading permissions
GRANT USAGE, CREATE ON SCHEMA grantor_schema TO role_1 WITH GRANT OPTION;
GRANT CREATE ON SCHEMA grantor_schema TO PUBLIC;
SET ROLE role_1;
GRANT USAGE ON SCHEMA grantor_schema TO role_2 WITH GRANT OPTION;
GRANT CREATE ON SCHEMA grantor_schema TO role_2;
GRANT USAGE, CREATE ON SCHEMA grantor_schema TO role_3;
GRANT CREATE, USAGE ON SCHEMA grantor_schema TO PUBLIC;
SET ROLE role_2;
GRANT USAGE ON SCHEMA grantor_schema TO role_3;
RESET ROLE;

-- distribute the schema
CREATE TABLE grantor_schema.grantor_table (id INT);
SELECT create_distributed_table('grantor_schema.grantor_table', 'id');

-- check if the grantors are propagated correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :master_port

-- add the previously removed node
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);

-- check if the grantors are propagated correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :worker_2_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :master_port

-- revoke one of the permissions
REVOKE USAGE ON SCHEMA grantor_schema FROM role_1 CASCADE;

-- check if revoke worked correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :master_port

-- test if grantor propagates correctly on already distributed schemas
GRANT USAGE ON SCHEMA grantor_schema TO role_1 WITH GRANT OPTION;
SET ROLE role_1;
GRANT USAGE ON SCHEMA grantor_schema TO role_2;
GRANT USAGE ON SCHEMA grantor_schema TO role_3 WITH GRANT OPTION;
SET ROLE role_3;
GRANT USAGE ON SCHEMA grantor_schema TO role_2;
RESET ROLE;

-- check the results
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'grantor_schema' ORDER BY nspname;
\c - - - :master_port

DROP TABLE grantor_schema.grantor_table;
SELECT run_command_on_coordinator_and_workers('DROP SCHEMA grantor_schema CASCADE');

-- test distributing the schema with another user
CREATE SCHEMA dist_schema;

GRANT ALL ON SCHEMA dist_schema TO role_1 WITH GRANT OPTION;
SET ROLE role_1;
GRANT ALL ON SCHEMA dist_schema TO role_2 WITH GRANT OPTION;

CREATE TABLE dist_schema.dist_table (id int);
SELECT create_distributed_table('dist_schema.dist_table', 'id');

SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'dist_schema' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'dist_schema' ORDER BY nspname;
\c - - - :master_port

DROP TABLE dist_schema.dist_table;
SELECT run_command_on_coordinator_and_workers('DROP SCHEMA dist_schema CASCADE');

-- test grants on public schema
-- first remove one of the worker nodes
SET citus.shard_replication_factor TO 1;
SELECT master_remove_node(:'worker_2_host', :worker_2_port);

-- distribute the public schema (it has to be distributed by now but just in case)
CREATE TABLE public_schema_table (id INT);
SELECT create_distributed_table('public_schema_table', 'id');

-- give cascading permissions
GRANT USAGE, CREATE ON SCHEMA PUBLIC TO role_1 WITH GRANT OPTION;
SET ROLE role_1;
GRANT USAGE ON SCHEMA PUBLIC TO PUBLIC;
RESET ROLE;

-- check if the grants are propagated correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :master_port

-- add the previously removed node
SELECT 1 FROM master_add_node(:'worker_2_host', :worker_2_port);

-- check if the grants are propagated correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :worker_2_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :master_port

-- revoke those new permissions
REVOKE CREATE, USAGE ON SCHEMA PUBLIC FROM role_1 CASCADE;

-- check if the grants are propagated correctly
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :worker_1_port
SELECT nspname, nspacl FROM pg_namespace WHERE nspname = 'public' ORDER BY nspname;
\c - - - :master_port

DROP TABLE public_schema_table;

SELECT run_command_on_coordinator_and_workers('DROP ROLE role_1, role_2, role_3');
