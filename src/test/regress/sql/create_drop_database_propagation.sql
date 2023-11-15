
-- Test for create/drop database propagation.
-- This test is only executes for Postgres versions < 15.
-- For versions >= 15, pg15_create_drop_database_propagation.sql is used.
-- For versions >= 16, pg16_create_drop_database_propagation.sql is used.

-- Test the UDF that we use to issue database command during metadata sync.
SELECT pg_catalog.citus_internal_database_command(null);

CREATE ROLE test_db_commands WITH LOGIN;
ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'test_db_commands';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
SET ROLE test_db_commands;

-- fails on null input
SELECT pg_catalog.citus_internal_database_command(null);

-- fails on non create / drop db command
SELECT pg_catalog.citus_internal_database_command('CREATE TABLE foo_bar(a int)');
SELECT pg_catalog.citus_internal_database_command('SELECT 1');
SELECT pg_catalog.citus_internal_database_command('asfsfdsg');
SELECT pg_catalog.citus_internal_database_command('');

RESET ROLE;
ALTER ROLE test_db_commands nocreatedb;
SET ROLE test_db_commands;

-- make sure that pg_catalog.citus_internal_database_command doesn't cause privilege escalation
SELECT pg_catalog.citus_internal_database_command('CREATE DATABASE no_permissions');

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
-- Today we don't support ALTER DATABASE .. RENAME TO .., so need to propagate it manually.
SELECT result FROM run_command_on_all_nodes(
  $$
  ALTER DATABASE "db-needs\!escape" RENAME TO db_needs_escape
  $$
);

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
set citus.grep_remote_commands = '%CREATE DATABASE%';

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

-- show that dropping the database from workers is not allowed when citus.enable_create_database_propagation is on
DROP DATABASE db_needs_escape;

-- and the same applies to create database too
create database error_test;

\c - - - :master_port

SET citus.enable_create_database_propagation TO ON;

DROP DATABASE test_node_activation;
DROP DATABASE db_needs_escape;
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
  revoke connect,temp,temporary,create  on database db_role_grants_test_non_distributed from public
  $$
) ORDER BY result;

SET citus.enable_create_database_propagation TO on;



CREATE ROLE db_role_grants_test_role_exists_on_node_2;


select 1 from citus_remove_node('localhost', :worker_2_port);

CREATE DATABASE db_role_grants_test;

revoke connect,temp,temporary,create  on database db_role_grants_test from public;

SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%CREATE ROLE%';
CREATE ROLE db_role_grants_test_role_missing_on_node_2;

RESET citus.log_remote_commands ;
RESET citus.grep_remote_commands;



SET citus.log_remote_commands = true;
set citus.grep_remote_commands = '%GRANT%';
grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test to db_role_grants_test_role_exists_on_node_2;
grant CONNECT,TEMPORARY,CREATE  on DATABASE db_role_grants_test to db_role_grants_test_role_missing_on_node_2;



grant CONNECT,TEMPORARY,CREATE on DATABASE db_role_grants_test_non_distributed to db_role_grants_test_role_exists_on_node_2;
grant CONNECT,TEMPORARY,CREATE  on DATABASE db_role_grants_test_non_distributed to db_role_grants_test_role_missing_on_node_2;

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

grant connect,temp,temporary,create  on database db_role_grants_test to public;

DROP DATABASE db_role_grants_test;

SELECT result from run_command_on_all_nodes(
  $$
  drop database db_role_grants_test_non_distributed
  $$
) ORDER BY result;
DROP ROLE db_role_grants_test_role_exists_on_node_2;
DROP ROLE db_role_grants_test_role_missing_on_node_2;




--clean up resources created by this test

-- DROP TABLESPACE is not supported, so we need to drop it manually.
SELECT result FROM run_command_on_all_nodes(
  $$
  drop tablespace "ts-needs\!escape"
  $$
);

drop user create_drop_db_test_user;
