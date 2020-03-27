CREATE SCHEMA alter_role;

SET citus.enable_alter_role_propagation to ON;

CREATE ROLE alter_role_1 WITH LOGIN;
SELECT run_command_on_workers($$CREATE ROLE alter_role_1 WITH LOGIN;$$);

-- postgres errors out
ALTER ROLE alter_role_1 WITH SUPERUSER NOSUPERUSER;

-- make sure that we propagate all options accurately
ALTER ROLE alter_role_1 WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 66 VALID UNTIL '2032-05-05';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1'$$);

-- make sure that we propagate all options accurately
ALTER ROLE alter_role_1 WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 0 VALID UNTIL '2052-05-05';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1'$$);

-- make sure that non-existent users are handled properly
ALTER ROLE alter_role_2 WITH SUPERUSER NOSUPERUSER;
ALTER ROLE alter_role_2 WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 66 VALID UNTIL '2032-05-05';

-- make sure that CURRENT_USER just works fine
ALTER ROLE CURRENT_USER WITH CONNECTION LIMIT 123;
SELECT rolconnlimit FROM pg_authid WHERE rolname = CURRENT_USER;
SELECT run_command_on_workers($$SELECT rolconnlimit FROM pg_authid WHERE rolname = CURRENT_USER;$$);

-- make sure that SESSION_USER just works fine
ALTER ROLE SESSION_USER WITH CONNECTION LIMIT 124;
SELECT rolconnlimit FROM pg_authid WHERE rolname = SESSION_USER;
SELECT run_command_on_workers($$SELECT rolconnlimit FROM pg_authid WHERE rolname = SESSION_USER;$$);

-- now lets test the passwords in more detail
ALTER ROLE alter_role_1 WITH PASSWORD NULL;
SELECT rolpassword is NULL FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT rolpassword is NULL FROM pg_authid WHERE rolname = 'alter_role_1'$$);

ALTER ROLE alter_role_1 WITH PASSWORD 'test1';
SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1'$$);

ALTER ROLE  alter_role_1 WITH ENCRYPTED PASSWORD 'test2';
SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1'$$);

ALTER ROLE  alter_role_1 WITH ENCRYPTED PASSWORD 'md59cce240038b7b335c6aa9674a6f13e72';
SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT rolpassword FROM pg_authid WHERE rolname = 'alter_role_1'$$);


-- edge case role names

CREATE ROLE "alter_role'1" WITH LOGIN;
SELECT run_command_on_workers($$CREATE ROLE "alter_role'1" WITH LOGIN;$$);
ALTER ROLE "alter_role'1" CREATEROLE;
SELECT rolcreaterole FROM pg_authid WHERE rolname = 'alter_role''1';
SELECT run_command_on_workers($$SELECT rolcreaterole FROM pg_authid WHERE rolname = 'alter_role''1'$$);

CREATE ROLE "alter_role""1" WITH LOGIN;
SELECT run_command_on_workers($$CREATE ROLE "alter_role""1" WITH LOGIN;$$);
ALTER ROLE "alter_role""1" CREATEROLE;
SELECT rolcreaterole FROM pg_authid WHERE rolname = 'alter_role"1';
SELECT run_command_on_workers($$SELECT rolcreaterole FROM pg_authid WHERE rolname = 'alter_role"1'$$);


-- add node
ALTER ROLE alter_role_1 WITH SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION LIMIT 66 VALID UNTIL '2032-05-05' PASSWORD 'test3';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1'$$);
SELECT master_remove_node('localhost', :worker_1_port);
ALTER ROLE alter_role_1 WITH NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 0 VALID UNTIL '2052-05-05' PASSWORD 'test4';
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1'$$);
SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1';
SELECT run_command_on_workers($$SELECT row(rolname, rolsuper, rolinherit,  rolcreaterole, rolcreatedb, rolcanlogin, rolreplication, rolbypassrls, rolconnlimit, rolpassword, EXTRACT (year FROM rolvaliduntil)) FROM pg_authid WHERE rolname = 'alter_role_1'$$);


-- give login permissions so that we can connect and check if the previous queries were propagated
ALTER ROLE alter_role_1 LOGIN CONNECTION LIMIT 10;


-- alter configuration_parameter defaults for a user
ALTER ROLE CURRENT_USER SET enable_hashagg TO FALSE;
SELECT run_command_on_workers('SHOW enable_hashagg');

-- reset to default values
ALTER ROLE CURRENT_USER RESET enable_hashagg;
SELECT run_command_on_workers('SHOW enable_hashagg');

-- provide role and database names
ALTER ROLE alter_role_1 IN DATABASE regression SET enable_hashjoin TO 0;

SET ROLE alter_role_1;
SELECT run_command_on_workers('SHOW enable_hashjoin');

-- make sure that only alter_role_1 was affected
RESET ROLE;
SELECT run_command_on_workers('SHOW enable_hashjoin');

-- RESET ALL with IN DATABASE clause
ALTER ROLE alter_role_1 IN DATABASE regression RESET ALL;
ALTER ROLE alter_role_1 RESET ALL;
ALTER ROLE ALL RESET ALL;

-- FROM CURRENT clauses
SET statement_timeout TO '1min';
ALTER ROLE alter_role_1 SET statement_timeout FROM CURRENT;

SET ROLE alter_role_1;
SELECT run_command_on_workers('SHOW statement_timeout');

RESET statement_timeout;
RESET ROLE;

-- the session defaults should be updated on master_add_node
SELECT master_remove_node('localhost', :worker_1_port);
ALTER ROLE SESSION_USER SET enable_mergejoin TO false;
ALTER ROLE CURRENT_USER SET statement_timeout TO '2min';
ALTER ROLE CURRENT_USER SET log_min_duration_statement TO '123s';
ALTER ROLE CURRENT_USER SET "app.dev""" TO 'a\nb';
ALTER ROLE CURRENT_USER SET myvar.foobar TO "007";

SELECT 1 FROM master_add_node('localhost', :worker_1_port);
SELECT run_command_on_workers('SHOW enable_mergejoin');
SELECT run_command_on_workers('SHOW statement_timeout');
SELECT run_command_on_workers('SHOW log_min_duration_statement');
SELECT run_command_on_workers('SHOW "app.dev"""');
SELECT run_command_on_workers('SHOW myvar.foobar');

-- revert back to defaults
ALTER ROLE SESSION_USER RESET ALL;
SELECT run_command_on_workers('SHOW enable_mergejoin');
SELECT run_command_on_workers('SHOW statement_timeout');
SELECT run_command_on_workers('SHOW log_min_duration_statement');
SELECT run_command_on_workers('SHOW "app.dev"""');
SELECT run_command_on_workers('SHOW myvar.foobar');

-- we don't support propagation of ALTER ROLE ... RENAME TO commands.
ALTER ROLE alter_role_1 RENAME TO alter_role_1_new;

SET citus.enable_alter_role_propagation to OFF;

DROP SCHEMA alter_role CASCADE;
