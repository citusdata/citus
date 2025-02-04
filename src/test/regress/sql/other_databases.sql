CREATE SCHEMA other_databases;
SET search_path TO other_databases;

SET citus.next_shard_id TO 10231023;

CREATE DATABASE other_db1;

\c other_db1
SHOW citus.main_db;

-- check that empty citus.superuser gives error
SET citus.superuser TO '';
CREATE USER empty_superuser;
SET citus.superuser TO 'postgres';

CREATE USER other_db_user1;
CREATE USER other_db_user2;

BEGIN;
CREATE USER other_db_user3;
CREATE USER other_db_user4;
COMMIT;

BEGIN;
CREATE USER other_db_user5;
CREATE USER other_db_user6;
ROLLBACK;

BEGIN;
CREATE USER other_db_user7;
SELECT 1/0;
COMMIT;

CREATE USER other_db_user8;

\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :worker_1_port
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :master_port
-- some user creation commands will fail but let's make sure we try to drop them just in case
DROP USER IF EXISTS other_db_user1, other_db_user2, other_db_user3, other_db_user4, other_db_user5, other_db_user6, other_db_user7, other_db_user8;

-- Make sure non-superuser roles cannot use internal GUCs
-- but they can still create a role
CREATE USER nonsuperuser CREATEROLE;
GRANT ALL ON SCHEMA citus_internal TO nonsuperuser;
SET ROLE nonsuperuser;
SELECT citus_internal.execute_command_on_remote_nodes_as_user($$SELECT 'dangerous query'$$, 'postgres');

\c other_db1
SET citus.local_hostname TO '127.0.0.1';
SET ROLE nonsuperuser;

-- Make sure that we don't try to access pg_dist_node.
-- Otherwise, we would get the following error:
--   ERROR:  cache lookup failed for pg_dist_node, called too early?
CREATE USER other_db_user9;

RESET ROLE;
RESET citus.local_hostname;
RESET ROLE;
\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :worker_1_port
SELECT usename FROM pg_user WHERE usename LIKE 'other\_db\_user%' ORDER BY 1;

\c - - - :master_port
REVOKE ALL ON SCHEMA citus_internal FROM nonsuperuser;
DROP USER other_db_user9, nonsuperuser;

-- test from a worker
\c - - - :worker_1_port

CREATE DATABASE worker_other_db;

\c worker_other_db

CREATE USER worker_user1;

BEGIN;
CREATE USER worker_user2;
COMMIT;

BEGIN;
CREATE USER worker_user3;
ROLLBACK;

\c regression
SELECT usename FROM pg_user WHERE usename LIKE 'worker\_user%' ORDER BY 1;

\c - - - :master_port
SELECT usename FROM pg_user WHERE usename LIKE 'worker\_user%' ORDER BY 1;

-- some user creation commands will fail but let's make sure we try to drop them just in case
DROP USER IF EXISTS worker_user1, worker_user2, worker_user3;

-- test creating and dropping a database from a Citus non-main database
SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO true$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);
\c other_db1
CREATE DATABASE other_db3;

\c regression
SELECT * FROM public.check_database_on_all_nodes('other_db3') ORDER BY node_type;

\c other_db1
DROP DATABASE other_db3;

\c regression
SELECT * FROM public.check_database_on_all_nodes('other_db3') ORDER BY node_type;

\c worker_other_db - - :worker_1_port
CREATE DATABASE other_db4;

\c regression
SELECT * FROM public.check_database_on_all_nodes('other_db4') ORDER BY node_type;

\c worker_other_db
DROP DATABASE other_db4;

\c regression
SELECT * FROM public.check_database_on_all_nodes('other_db4') ORDER BY node_type;

DROP DATABASE worker_other_db;

CREATE DATABASE other_db5;

-- disable create database propagation for the next test
SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO false$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);

\c other_db5 - - :worker_2_port

-- locally create a database
CREATE DATABASE local_db;

\c regression - - -

-- re-enable create database propagation
SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO true$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);

\c other_db5 - - :master_port

-- Test a scenario where create database fails because the database
-- already exists on another node and we don't crash etc.
CREATE DATABASE local_db;

\c regression - - -

SELECT * FROM public.check_database_on_all_nodes('local_db') ORDER BY node_type, result;

\c - - - :worker_2_port

-- locally drop the database for cleanup purposes
SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO false$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);

DROP DATABASE local_db;

SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO true$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);

\c - - - :master_port

DROP DATABASE other_db5;

SELECT result FROM run_command_on_all_nodes($$ALTER SYSTEM SET citus.enable_create_database_propagation TO false$$);
SELECT result FROM run_command_on_all_nodes($$SELECT pg_reload_conf()$$);
SELECT pg_sleep(0.1);

DROP SCHEMA other_databases;
DROP DATABASE other_db1;
