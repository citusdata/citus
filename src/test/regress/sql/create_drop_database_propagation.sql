
-- Test for create/drop database propagation.
-- This test is only executes for Postgres versions < 15.
-- For versions >= 15, pg15_create_drop_database_propagation.sql is used.
-- For versions >= 16, pg16_create_drop_database_propagation.sql is used.

-- Test the UDF that we use to issue database command during metadata sync.
SELECT citus_internal.database_command(null);

CREATE ROLE test_db_commands WITH LOGIN;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'test_db_commands';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET ROLE test_db_commands;

-- fails on null input
SELECT citus_internal.database_command(null);

-- fails on non create / drop db command
SELECT citus_internal.database_command('CREATE TABLE foo_bar(a int)');
SELECT citus_internal.database_command('SELECT 1');
SELECT citus_internal.database_command('asfsfdsg');
SELECT citus_internal.database_command('');

RESET ROLE;
ALTER ROLE test_db_commands nocreatedb;
SET ROLE test_db_commands;

-- make sure that citus_internal.database_command doesn't cause privilege escalation
SELECT citus_internal.database_command('CREATE DATABASE no_permissions');

RESET ROLE;
DROP USER test_db_commands;
ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts3'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :worker_1_port
\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts4'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :worker_2_port
\set create_drop_db_tablespace :abs_srcdir '/tmp_check/ts5'
CREATE TABLESPACE create_drop_db_tablespace LOCATION :'create_drop_db_tablespace';

\c - - - :master_port
CREATE DATABASE local_database;

-- check that it's only created for coordinator
SELECT * FROM public.check_database_on_all_nodes('local_database') ORDER BY node_type;

DROP DATABASE local_database;

-- and is dropped
SELECT * FROM public.check_database_on_all_nodes('local_database') ORDER BY node_type;

\c - - - :worker_1_port
CREATE DATABASE local_database;

-- check that it's only created for coordinator
SELECT * FROM public.check_database_on_all_nodes('local_database') ORDER BY node_type;

DROP DATABASE local_database;

-- and is dropped
SELECT * FROM public.check_database_on_all_nodes('local_database') ORDER BY node_type;

\c - - - :master_port
create user create_drop_db_test_user;

set citus.enable_create_database_propagation=on;

-- Tests for create database propagation with template0 which should fail
CREATE DATABASE mydatabase
    WITH OWNER = create_drop_db_test_user
    TEMPLATE = 'template0'
            ENCODING = 'UTF8'
            CONNECTION LIMIT = 10
            TABLESPACE = create_drop_db_tablespace
            ALLOW_CONNECTIONS = true
            IS_TEMPLATE = false;

CREATE DATABASE mydatabase_1
    WITH template=template1
    OWNER = create_drop_db_test_user
            ENCODING = 'UTF8'
            CONNECTION LIMIT = 10
            TABLESPACE = create_drop_db_tablespace
            ALLOW_CONNECTIONS = true
            IS_TEMPLATE = false;

SELECT * FROM public.check_database_on_all_nodes('mydatabase_1') ORDER BY node_type;

-- Test LC / LOCALE settings that don't match the ones provided in template db.
-- All should throw an error on the coordinator.
CREATE DATABASE lc_collate_test LC_COLLATE = 'C.UTF-8';
CREATE DATABASE lc_ctype_test LC_CTYPE = 'C.UTF-8';
CREATE DATABASE locale_test LOCALE = 'C.UTF-8';
CREATE DATABASE lc_collate_lc_ctype_test LC_COLLATE = 'C.UTF-8' LC_CTYPE = 'C.UTF-8';

-- Test LC / LOCALE settings that match the ones provided in template db.
CREATE DATABASE lc_collate_test LC_COLLATE = 'C';
CREATE DATABASE lc_ctype_test LC_CTYPE = 'C';
CREATE DATABASE locale_test LOCALE = 'C';
CREATE DATABASE lc_collate_lc_ctype_test LC_COLLATE = 'C' LC_CTYPE = 'C';

SELECT * FROM public.check_database_on_all_nodes('lc_collate_test') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('lc_ctype_test') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('locale_test') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('lc_collate_lc_ctype_test') ORDER BY node_type;

DROP DATABASE lc_collate_test;
DROP DATABASE lc_ctype_test;
DROP DATABASE locale_test;
DROP DATABASE lc_collate_lc_ctype_test;

-- ALTER TABLESPACE .. RENAME TO .. is not supported, so we need to rename it manually.
SELECT result FROM run_command_on_all_nodes(
  $$
  ALTER TABLESPACE create_drop_db_tablespace RENAME TO "ts-needs\!escape"
  $$
);

CREATE USER "role-needs\!escape";

CREATE DATABASE "db-needs\!escape" owner "role-needs\!escape" tablespace "ts-needs\!escape";

-- Rename it to make check_database_on_all_nodes happy.

ALTER DATABASE "db-needs\!escape" RENAME TO db_needs_escape;
SELECT * FROM public.check_database_on_all_nodes('db_needs_escape') ORDER BY node_type;

-- test database syncing after node addition

select 1 from citus_remove_node('localhost', :worker_2_port);

--test with is_template true and allow connections false
CREATE DATABASE mydatabase
            OWNER = create_drop_db_test_user
            CONNECTION LIMIT = 10
            ENCODING = 'UTF8'
            TABLESPACE = "ts-needs\!escape"
            ALLOW_CONNECTIONS = false
            IS_TEMPLATE = false;

SELECT * FROM public.check_database_on_all_nodes('mydatabase') ORDER BY node_type;

SET citus.metadata_sync_mode to 'transactional';
select 1 from citus_add_node('localhost', :worker_2_port);

SELECT * FROM public.check_database_on_all_nodes('mydatabase') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('mydatabase_1') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('db_needs_escape') ORDER BY node_type;

select 1 from citus_remove_node('localhost', :worker_2_port);

SET citus.metadata_sync_mode to 'nontransactional';
select 1 from citus_add_node('localhost', :worker_2_port);

RESET citus.metadata_sync_mode;

SELECT * FROM public.check_database_on_all_nodes('mydatabase') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('mydatabase_1') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('db_needs_escape') ORDER BY node_type;

SELECT citus_disable_node_and_wait('localhost', :worker_1_port, true);

CREATE DATABASE test_node_activation;
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);

SELECT * FROM public.check_database_on_all_nodes('mydatabase') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('mydatabase_1') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('db_needs_escape') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('test_node_activation') ORDER BY node_type;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DROP DATABASE%';
drop database mydatabase;

SET citus.log_remote_commands = false;

-- check that we actually drop the database
drop database mydatabase_1;

SELECT * FROM public.check_database_on_all_nodes('mydatabase_1') ORDER BY node_type;

SELECT * FROM public.check_database_on_all_nodes('mydatabase') ORDER BY node_type;

-- create a template database with all options set and allow connections false
CREATE DATABASE my_template_database
    WITH    OWNER = create_drop_db_test_user
            ENCODING = 'UTF8'
            TABLESPACE = "ts-needs\!escape"
            ALLOW_CONNECTIONS = false
            IS_TEMPLATE = true;

SELECT * FROM public.check_database_on_all_nodes('my_template_database') ORDER BY node_type;

--template databases could not be dropped so we need to change the template flag
SELECT result from run_command_on_all_nodes(
  $$
  UPDATE pg_database SET datistemplate = false WHERE datname = 'my_template_database'
  $$
) ORDER BY result;

SET citus.log_remote_commands = true;

set citus.grep_remote_commands = '%DROP DATABASE%';
drop database my_template_database;

SET citus.log_remote_commands = false;

SELECT * FROM public.check_database_on_all_nodes('my_template_database') ORDER BY node_type;

--tests for special characters in database name
set citus.enable_create_database_propagation=on;
SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DATABASE%';
SET citus.next_operation_id TO 2000;

create database "mydatabase#1'2";

set citus.grep_remote_commands = '%DROP DATABASE%';
drop database if exists "mydatabase#1'2";

reset citus.grep_remote_commands;
reset citus.log_remote_commands;

-- it doesn't fail thanks to "if exists"
drop database if exists "mydatabase#1'2";

-- recreate it to verify that it's actually dropped
create database "mydatabase#1'2";
drop database "mydatabase#1'2";

-- second time we try to drop it, it fails due to lack of "if exists"
drop database "mydatabase#1'2";

\c - - - :worker_1_port

SET citus.enable_create_database_propagation TO ON;

-- show that dropping the database from workers is allowed when citus.enable_create_database_propagation is on
DROP DATABASE db_needs_escape;

-- and the same applies to create database too
create database error_test;
drop database error_test;

\c - - - :master_port

SET citus.enable_create_database_propagation TO ON;

DROP DATABASE test_node_activation;
DROP USER "role-needs\!escape";

-- drop database with force options test

create database db_force_test;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%DROP DATABASE%';

drop database db_force_test with (force);

reset citus.log_remote_commands;
reset citus.grep_remote_commands;

SELECT * FROM public.check_database_on_all_nodes('db_force_test') ORDER BY node_type;

-- test that we won't propagate non-distributed databases in citus_add_node

select 1 from citus_remove_node('localhost', :worker_2_port);
SET citus.enable_create_database_propagation TO off;
CREATE DATABASE non_distributed_db;
SET citus.enable_create_database_propagation TO on;
create database distributed_db;

select 1 from citus_add_node('localhost', :worker_2_port);

--non_distributed_db should not be propagated to worker_2
SELECT * FROM public.check_database_on_all_nodes('non_distributed_db') ORDER BY node_type;
--distributed_db should be propagated to worker_2
SELECT * FROM public.check_database_on_all_nodes('distributed_db') ORDER BY node_type;

--clean up resources created by this test
drop database distributed_db;

set citus.enable_create_database_propagation TO off;
drop database non_distributed_db;

-- test role grants on DATABASE in metadata sync

SELECT result from run_command_on_all_nodes(
  $$
  create database db_role_grants_test_non_distributed
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  revoke connect,temp,temporary,create on database db_role_grants_test_non_distributed from public
  $$
) ORDER BY result;

SET citus.enable_create_database_propagation TO on;

CREATE ROLE db_role_grants_test_role_exists_on_node_2;

select 1 from citus_remove_node('localhost', :worker_2_port);

CREATE DATABASE db_role_grants_test;

revoke connect,temp,temporary,create on database db_role_grants_test from public;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%CREATE ROLE%';
CREATE ROLE db_role_grants_test_role_missing_on_node_2;

RESET citus.log_remote_commands ;
RESET citus.grep_remote_commands;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%GRANT%';
grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test to db_role_grants_test_role_exists_on_node_2;
grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test to db_role_grants_test_role_missing_on_node_2;

grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test_non_distributed to db_role_grants_test_role_exists_on_node_2;
grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test_non_distributed to db_role_grants_test_role_missing_on_node_2;

-- check the privileges before add_node for database db_role_grants_test,
--  role db_role_grants_test_role_exists_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges before add_node for database db_role_grants_test,
--  role db_role_grants_test_role_missing_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges before add_node  for database db_role_grants_test_non_distributed,
--  role db_role_grants_test_role_exists_on_node_2
SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges before add_node for database db_role_grants_test_non_distributed,
--  role db_role_grants_test_role_missing_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'CONNECT')
  $$
) ORDER BY result;

RESET citus.log_remote_commands;
RESET citus.grep_remote_commands;

select 1 from citus_add_node('localhost', :worker_2_port);

-- check the privileges after add_node for database db_role_grants_test,
--  role db_role_grants_test_role_exists_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges after add_node for database db_role_grants_test,
--  role db_role_grants_test_role_missing_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges after add_node for database db_role_grants_test_non_distributed,
--  role db_role_grants_test_role_exists_on_node_2
SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_exists_on_node_2','db_role_grants_test_non_distributed', 'CONNECT')
  $$
) ORDER BY result;

-- check the privileges after add_node for database db_role_grants_test_non_distributed,
--  role db_role_grants_test_role_missing_on_node_2

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'CREATE')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'TEMPORARY')
  $$
) ORDER BY result;

SELECT result from run_command_on_all_nodes(
  $$
  select has_database_privilege('db_role_grants_test_role_missing_on_node_2','db_role_grants_test_non_distributed', 'CONNECT')
  $$
) ORDER BY result;

grant connect,temp,temporary,create on database db_role_grants_test to public;

DROP DATABASE db_role_grants_test;

SELECT result from run_command_on_all_nodes(
  $$
  drop database db_role_grants_test_non_distributed
  $$
) ORDER BY result;
DROP ROLE db_role_grants_test_role_exists_on_node_2;
DROP ROLE db_role_grants_test_role_missing_on_node_2;

select 1 from citus_remove_node('localhost', :worker_2_port);

set citus.enable_create_role_propagation TO off;
create role non_propagated_role;
set citus.enable_create_role_propagation TO on;

set citus.enable_create_database_propagation TO on;

-- Make sure that we propagate non_propagated_role because it's a dependency of test_db.
-- And hence it becomes a distributed object.
create database test_db OWNER non_propagated_role;

create role propagated_role;
grant connect on database test_db to propagated_role;

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT * FROM public.check_database_on_all_nodes('test_db') ORDER BY node_type;

REVOKE CONNECT ON DATABASE test_db FROM propagated_role;
DROP DATABASE test_db;
DROP ROLE propagated_role, non_propagated_role;

-- show that we don't try to propagate commands on non-distributed databases
SET citus.enable_create_database_propagation TO OFF;
CREATE DATABASE local_database_1;
SET citus.enable_create_database_propagation TO ON;

CREATE ROLE local_role_1;

GRANT CONNECT, TEMPORARY, CREATE ON DATABASE local_database_1 TO local_role_1;
ALTER DATABASE local_database_1 SET default_transaction_read_only = 'true';

REVOKE CONNECT, TEMPORARY, CREATE ON DATABASE local_database_1 FROM local_role_1;
DROP ROLE local_role_1;
DROP DATABASE local_database_1;

-- test create / drop database commands from workers

-- remove one of the workers to test node activation too
SELECT 1 from citus_remove_node('localhost', :worker_2_port);

\c - - - :worker_1_port

CREATE DATABASE local_worker_db;

SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE db_created_from_worker
    WITH template=template1
    OWNER = create_drop_db_test_user
            ENCODING = 'UTF8'
            CONNECTION LIMIT = 42
            TABLESPACE = "ts-needs\!escape"
            ALLOW_CONNECTIONS = false;

\c - - - :master_port

SET citus.enable_create_database_propagation TO ON;

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

\c - - - :worker_1_port

SET citus.enable_create_database_propagation TO ON;

SELECT * FROM public.check_database_on_all_nodes('local_worker_db') ORDER BY node_type;
SELECT * FROM public.check_database_on_all_nodes('db_created_from_worker') ORDER BY node_type;

DROP DATABASE db_created_from_worker;

SELECT * FROM public.check_database_on_all_nodes('db_created_from_worker') ORDER BY node_type;

-- drop the local database while the GUC is on
DROP DATABASE local_worker_db;
SELECT * FROM public.check_database_on_all_nodes('local_worker_db') ORDER BY node_type;

SET citus.enable_create_database_propagation TO OFF;

CREATE DATABASE local_worker_db;

-- drop the local database while the GUC is off
DROP DATABASE local_worker_db;
SELECT * FROM public.check_database_on_all_nodes('local_worker_db') ORDER BY node_type;

SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE another_db_created_from_worker;

\c - - - :master_port

SELECT 1 FROM citus_remove_node('localhost', :master_port);

\c - - - :worker_1_port

SET citus.enable_create_database_propagation TO ON;

-- fails because coordinator is not added into metadata
DROP DATABASE another_db_created_from_worker;

-- fails because coordinator is not added into metadata
CREATE DATABASE new_db;

\c - - - :master_port

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, 0);
RESET client_min_messages;

SET citus.enable_create_database_propagation TO ON;

-- dropping a database that was created from a worker via a different node works fine
DROP DATABASE another_db_created_from_worker;
SELECT * FROM public.check_database_on_all_nodes('another_db_created_from_worker') ORDER BY node_type;

-- Show that we automatically propagate the dependencies (only roles atm) when
-- creating a database from workers too.

SELECT 1 from citus_remove_node('localhost', :worker_2_port);

\c - - - :worker_1_port

set citus.enable_create_role_propagation TO off;
create role non_propagated_role;
set citus.enable_create_role_propagation TO on;

set citus.enable_create_database_propagation TO on;

create database test_db OWNER non_propagated_role;

create role propagated_role;

\c - - - :master_port

-- not supported from workers, so need to execute this via coordinator
grant connect on database test_db to propagated_role;

SET citus.enable_create_database_propagation TO ON;

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT * FROM public.check_database_on_all_nodes('test_db') ORDER BY node_type;

REVOKE CONNECT ON DATABASE test_db FROM propagated_role;
DROP DATABASE test_db;
DROP ROLE propagated_role, non_propagated_role;

-- test citus_internal.acquire_citus_advisory_object_class_lock with null input
SELECT citus_internal.acquire_citus_advisory_object_class_lock(null, 'regression');
SELECT citus_internal.acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), null);

-- OCLASS_DATABASE
SELECT citus_internal.acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), NULL);
SELECT citus_internal.acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), 'regression');
SELECT citus_internal.acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), '');
SELECT citus_internal.acquire_citus_advisory_object_class_lock((SELECT CASE WHEN substring(version(), '\d+')::integer < 16 THEN 25 ELSE 26 END AS oclass_database), 'no_such_db');

-- invalid OCLASS
SELECT citus_internal.acquire_citus_advisory_object_class_lock(-1, NULL);
SELECT citus_internal.acquire_citus_advisory_object_class_lock(-1, 'regression');

-- invalid OCLASS
SELECT citus_internal.acquire_citus_advisory_object_class_lock(100, NULL);
SELECT citus_internal.acquire_citus_advisory_object_class_lock(100, 'regression');

-- another valid OCLASS, but not implemented yet
SELECT citus_internal.acquire_citus_advisory_object_class_lock(10, NULL);
SELECT citus_internal.acquire_citus_advisory_object_class_lock(10, 'regression');

SELECT 1 FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_create_database_propagation TO ON');
SELECT 1 FROM run_command_on_all_nodes('SELECT pg_reload_conf()');
SELECT pg_sleep(0.1);

-- only one of them succeeds and we don't run into a distributed deadlock
SELECT COUNT(*) FROM run_command_on_all_nodes('CREATE DATABASE concurrent_create_db') WHERE success;
SELECT * FROM public.check_database_on_all_nodes('concurrent_create_db') ORDER BY node_type;

SELECT COUNT(*) FROM run_command_on_all_nodes('DROP DATABASE concurrent_create_db') WHERE success;
SELECT * FROM public.check_database_on_all_nodes('concurrent_create_db') ORDER BY node_type;

-- revert the system wide change that enables citus.enable_create_database_propagation on all nodes
SELECT 1 FROM run_command_on_all_nodes('ALTER SYSTEM SET citus.enable_create_database_propagation TO OFF');
SELECT 1 FROM run_command_on_all_nodes('SELECT pg_reload_conf()');
SELECT pg_sleep(0.1);

-- but keep it enabled for coordinator for the rest of the tests
SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE distributed_db;

CREATE USER no_createdb;
SET ROLE no_createdb;
SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE no_createdb;
ALTER DATABASE distributed_db RENAME TO rename_test;
DROP DATABASE distributed_db;
ALTER DATABASE distributed_db SET TABLESPACE pg_default;
ALTER DATABASE distributed_db SET timezone TO 'UTC';
ALTER DATABASE distributed_db RESET timezone;
GRANT ALL ON DATABASE distributed_db TO postgres;

RESET ROLE;

ALTER ROLE no_createdb createdb;

SET ROLE no_createdb;

CREATE DATABASE no_createdb;

ALTER DATABASE distributed_db RENAME TO rename_test;

RESET ROLE;

SELECT 1 FROM run_command_on_all_nodes($$GRANT ALL ON TABLESPACE pg_default TO no_createdb$$);
ALTER DATABASE distributed_db OWNER TO no_createdb;

SET ROLE no_createdb;

ALTER DATABASE distributed_db SET TABLESPACE pg_default;
ALTER DATABASE distributed_db SET timezone TO 'UTC';
ALTER DATABASE distributed_db RESET timezone;
GRANT ALL ON DATABASE distributed_db TO postgres;
ALTER DATABASE distributed_db RENAME TO rename_test;
DROP DATABASE rename_test;

RESET ROLE;

SELECT 1 FROM run_command_on_all_nodes($$REVOKE ALL ON TABLESPACE pg_default FROM no_createdb$$);

DROP DATABASE no_createdb;
DROP USER no_createdb;

-- Test a failure scenario by trying to create a distributed database that
-- already exists on one of the nodes.

\c - - - :worker_1_port
CREATE DATABASE "test_\!failure";

\c - - - :master_port

SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE "test_\!failure";

SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
RESET client_min_messages;

SELECT result AS database_cleanedup_on_node FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$test_\!failure$$) ORDER BY node_type, result;

SET citus.enable_create_database_propagation TO OFF;
CREATE DATABASE "test_\!failure1";

\c - - - :worker_1_port
DROP DATABASE "test_\!failure";

SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE "test_\!failure1";

SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
RESET client_min_messages;

SELECT result AS database_cleanedup_on_node FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$test_\!failure1$$) ORDER BY node_type, result;

\c - - - :master_port

-- Before dropping local "test_\!failure1" database, test a failure scenario
-- by trying to create a distributed database that already exists "on local
-- node" this time.

SET citus.enable_create_database_propagation TO ON;

CREATE DATABASE "test_\!failure1";

SET client_min_messages TO WARNING;
CALL citus_cleanup_orphaned_resources();
RESET client_min_messages;

SELECT result AS database_cleanedup_on_node FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$test_\!failure1$$) ORDER BY node_type, result;

SET citus.enable_create_database_propagation TO OFF;

DROP DATABASE "test_\!failure1";

SET citus.enable_create_database_propagation TO ON;

--clean up resources created by this test

-- DROP TABLESPACE is not supported, so we need to drop it manually.
SELECT result FROM run_command_on_all_nodes(
  $$
  drop tablespace "ts-needs\!escape"
  $$
);

drop user create_drop_db_test_user;
reset citus.enable_create_database_propagation;
