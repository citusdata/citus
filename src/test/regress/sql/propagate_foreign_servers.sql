CREATE SCHEMA propagate_foreign_server;
SET search_path TO propagate_foreign_server;

-- remove node to add later
SELECT citus_remove_node('localhost', :worker_1_port);

-- create schema, extension and foreign server while the worker is removed
SET citus.enable_ddl_propagation TO OFF;
CREATE SCHEMA test_dependent_schema;
CREATE EXTENSION postgres_fdw WITH SCHEMA test_dependent_schema;
SET citus.enable_ddl_propagation TO ON;
CREATE SERVER foreign_server_dependent_schema
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'test');
CREATE FOREIGN TABLE foreign_table (
        id integer NOT NULL,
        data text
)
        SERVER foreign_server_dependent_schema
        OPTIONS (schema_name 'test_dependent_schema', table_name 'foreign_table_test');

SELECT 1 FROM citus_add_node('localhost', :master_port, groupId=>0);
SELECT citus_add_local_table_to_metadata('foreign_table');
ALTER TABLE foreign_table OWNER TO pg_monitor;

SELECT 1 FROM citus_add_node('localhost', :worker_1_port);

-- verify the dependent schema and the foreign server are created on the newly added worker
SELECT run_command_on_workers(
        $$SELECT COUNT(*) FROM pg_namespace WHERE nspname = 'test_dependent_schema';$$);
SELECT run_command_on_workers(
        $$SELECT COUNT(*)=1 FROM pg_foreign_server WHERE srvname = 'foreign_server_dependent_schema';$$);

-- verify the owner is altered on workers
SELECT run_command_on_workers($$select r.rolname from pg_roles r join pg_class c on r.oid=c.relowner where relname = 'foreign_table';$$);

CREATE SERVER foreign_server_to_drop
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'test');

--should error
DROP SERVER foreign_server_dependent_schema, foreign_server_to_drop;
DROP FOREIGN TABLE foreign_table;
SELECT citus_remove_node('localhost', :master_port);

SET client_min_messages TO ERROR;
DROP SCHEMA test_dependent_schema CASCADE;
RESET client_min_messages;

-- test propagating foreign server creation
CREATE EXTENSION postgres_fdw;
CREATE SERVER foreign_server TYPE 'test_type' VERSION 'v1'
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host 'testhost', port '5432', dbname 'testdb');

SELECT COUNT(*)=1 FROM pg_foreign_server WHERE srvname = 'foreign_server';

-- verify that the server is created on the worker
SELECT run_command_on_workers(
        $$SELECT COUNT(*)=1 FROM pg_foreign_server WHERE srvname = 'foreign_server';$$);

ALTER SERVER foreign_server OPTIONS (ADD "fdw_startup_cost" '1000');
ALTER SERVER foreign_server OPTIONS (ADD passfile 'to_be_dropped');
ALTER SERVER foreign_server OPTIONS (SET host 'localhost');
ALTER SERVER foreign_server OPTIONS (DROP port, DROP dbname);
ALTER SERVER foreign_server OPTIONS (ADD port :'master_port', dbname 'regression', DROP passfile);
ALTER SERVER foreign_server RENAME TO "foreign'server_1!";

-- test alter owner
SELECT rolname FROM pg_roles JOIN pg_foreign_server ON (pg_roles.oid=pg_foreign_server.srvowner) WHERE srvname = 'foreign''server_1!';
ALTER SERVER "foreign'server_1!" OWNER TO pg_monitor;

-- verify that the server is renamed on the worker
SELECT run_command_on_workers(
        $$SELECT srvoptions FROM pg_foreign_server WHERE srvname = 'foreign''server_1!';$$);

-- verify the owner is changed
SELECT run_command_on_workers(
        $$SELECT rolname FROM pg_roles WHERE oid IN (SELECT srvowner FROM pg_foreign_server WHERE srvname = 'foreign''server_1!');$$);

-- verify the owner is changed on the coordinator
SELECT rolname FROM pg_roles JOIN pg_foreign_server ON (pg_roles.oid=pg_foreign_server.srvowner) WHERE srvname = 'foreign''server_1!';
DROP SERVER IF EXISTS "foreign'server_1!" CASCADE;

-- verify that the server is dropped on the worker
SELECT run_command_on_workers(
        $$SELECT COUNT(*)=0 FROM pg_foreign_server WHERE srvname = 'foreign''server_1!';$$);

\c - - - :worker_1_port
-- not allowed on the worker
ALTER SERVER foreign_server OPTIONS (ADD async_capable 'False');

CREATE SERVER foreign_server_1 TYPE 'test_type' VERSION 'v1'
         FOREIGN DATA WRAPPER postgres_fdw
         OPTIONS (host 'testhost', port '5432', dbname 'testdb');

\c - - - :master_port
DROP SCHEMA propagate_foreign_server CASCADE;
