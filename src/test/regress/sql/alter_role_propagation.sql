CREATE SCHEMA alter_role;
CREATE SCHEMA ",CitUs,.TeeN!?";

-- test if the passowrd of the extension owner can be upgraded
ALTER ROLE CURRENT_USER PASSWORD 'password123' VALID UNTIL 'infinity';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT workers.result = pg_authid.rolpassword AS password_is_same FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = current_user$$) workers, pg_authid WHERE pg_authid.rolname = current_user;

-- test if the password and some connection settings are propagated when a node gets added
ALTER ROLE CURRENT_USER WITH CONNECTION LIMIT 66 VALID UNTIL '2032-05-05' PASSWORD 'password456';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT workers.result = pg_authid.rolpassword AS password_is_same FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = current_user$$) workers, pg_authid WHERE pg_authid.rolname = current_user;
SELECT master_remove_node('localhost', :worker_1_port);
ALTER ROLE CURRENT_USER WITH CONNECTION LIMIT 0 VALID UNTIL '2052-05-05' PASSWORD 'password789';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT workers.result = pg_authid.rolpassword AS password_is_same FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = current_user$$) workers, pg_authid WHERE pg_authid.rolname = current_user;
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = current_user$$);
SELECT workers.result = pg_authid.rolpassword AS password_is_same FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = current_user$$) workers, pg_authid WHERE pg_authid.rolname = current_user;

-- check user, database and postgres wide SET settings.
-- pre check
SELECT run_command_on_workers('SHOW enable_hashjoin');
SELECT run_command_on_workers('SHOW enable_indexonlyscan');
SELECT run_command_on_workers('SHOW enable_hashagg');

-- remove 1 node to verify settings are copied when the node gets added back
SELECT master_remove_node('localhost', :worker_1_port);

-- change a setting for all users
ALTER ROLE ALL SET enable_hashjoin TO FALSE;
SELECT run_command_on_workers('SHOW enable_hashjoin');
ALTER ROLE ALL IN DATABASE regression SET enable_indexonlyscan TO FALSE;
SELECT run_command_on_workers('SHOW enable_indexonlyscan');

-- alter configuration_parameter defaults for a user
ALTER ROLE CURRENT_USER SET enable_hashagg TO FALSE;
SELECT run_command_on_workers('SHOW enable_hashagg');

-- provide a list of values in a supported configuration
ALTER ROLE CURRENT_USER SET search_path TO ",CitUs,.TeeN!?", alter_role, public;
-- test user defined GUCs that appear to be a list, but instead a single string
ALTER ROLE ALL SET public.myguc TO "Hello, World";

-- test for configuration values that should not be downcased even when unquoted
ALTER ROLE CURRENT_USER SET lc_messages TO 'C';

-- add worker and check all settings are copied
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT run_command_on_workers('SHOW enable_hashjoin');
SELECT run_command_on_workers('SHOW enable_indexonlyscan');
SELECT run_command_on_workers('SHOW enable_hashagg');
SELECT run_command_on_workers('SHOW search_path');
SELECT run_command_on_workers('SHOW lc_messages');
SELECT run_command_on_workers('SHOW public.myguc');

-- reset to default values
ALTER ROLE CURRENT_USER RESET enable_hashagg;
SELECT run_command_on_workers('SHOW enable_hashagg');

-- RESET ALL with IN DATABASE clause
ALTER ROLE ALL RESET ALL;

-- post check 1 - should have settings reset except for database specific settings
SELECT run_command_on_workers('SHOW enable_hashjoin');
SELECT run_command_on_workers('SHOW enable_indexonlyscan');
SELECT run_command_on_workers('SHOW enable_hashagg');

ALTER ROLE ALL IN DATABASE regression RESET ALL;

-- post check 2 - should have all settings reset
SELECT run_command_on_workers('SHOW enable_hashjoin');
SELECT run_command_on_workers('SHOW enable_indexonlyscan');
SELECT run_command_on_workers('SHOW enable_hashagg');

-- check that ALTER ROLE SET is not propagated when scoped to a different database
-- also test case sensitivity
CREATE DATABASE "REGRESSION";
ALTER ROLE CURRENT_USER IN DATABASE "REGRESSION" SET public.myguc TO "Hello from coordinator only";
SELECT d.datname, r.setconfig FROM pg_db_role_setting r LEFT JOIN pg_database d ON r.setdatabase=d.oid WHERE r.setconfig::text LIKE '%Hello from coordinator only%';
SELECT run_command_on_workers($$SELECT json_agg((d.datname, r.setconfig)) FROM pg_db_role_setting r LEFT JOIN pg_database d ON r.setdatabase=d.oid WHERE r.setconfig::text LIKE '%Hello from coordinator only%'$$);
DROP DATABASE "REGRESSION";

-- make sure alter role set is not propagated when the feature is deliberately turned off
SET citus.enable_alter_role_set_propagation TO off;
-- remove 1 node to verify settings are NOT copied when the node gets added back
SELECT master_remove_node('localhost', :worker_1_port);
ALTER ROLE ALL SET enable_hashjoin TO FALSE;
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT run_command_on_workers('SHOW enable_hashjoin');
ALTER ROLE ALL RESET enable_hashjoin;
SELECT run_command_on_workers('SHOW enable_hashjoin');

-- check altering search path won't cause public shards being not found
CREATE TABLE test_search_path(a int);
SELECT create_distributed_table('test_search_path', 'a');
CREATE SCHEMA test_sp;
ALTER USER current_user SET search_path TO test_sp;
SELECT COUNT(*) FROM public.test_search_path;
ALTER USER current_user RESET search_path;

-- test empty/null password: it is treated the same as no password
SET password_encryption TO md5;

CREATE ROLE new_role;
SELECT workers.result AS worker_password, pg_authid.rolpassword AS coord_password FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'new_role'$$) workers, pg_authid WHERE pg_authid.rolname = 'new_role';

ALTER ROLE new_role PASSWORD '';
SELECT workers.result AS worker_password, pg_authid.rolpassword AS coord_password FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'new_role'$$) workers, pg_authid WHERE pg_authid.rolname = 'new_role';

ALTER ROLE new_role PASSWORD 'new_password';
SELECT workers.result AS worker_password, pg_authid.rolpassword AS coord_password, workers.result = pg_authid.rolpassword AS password_is_same FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'new_role'$$) workers, pg_authid WHERE pg_authid.rolname = 'new_role';

ALTER ROLE new_role PASSWORD NULL;
SELECT workers.result AS worker_password, pg_authid.rolpassword AS coord_password FROM run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'new_role'$$) workers, pg_authid WHERE pg_authid.rolname = 'new_role';

RESET password_encryption;
DROP ROLE new_role;
DROP TABLE test_search_path;
DROP SCHEMA alter_role, ",CitUs,.TeeN!?", test_sp CASCADE;
