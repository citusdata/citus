-- Create a new database

set citus.enable_create_database_propagation to on;

CREATE DATABASE test_db;

-- Connect to the new database
\c test_db
-- Test CREATE ROLE with various options
CREATE ROLE test_role1 WITH LOGIN PASSWORD 'password1';

\c test_db - - :worker_1_port

CREATE USER "test_role2-needs\!escape"
WITH
    SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION
LIMIT 10 VALID UNTIL '2023-01-01' IN ROLE test_role1;

\c regression - - :master_port

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, rolvaliduntil
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY rolname
    ) t
$$);


select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY member::regrole::text
    ) t
$$);

\c test_db - - :master_port
-- Test ALTER ROLE with various options
ALTER ROLE test_role1 WITH PASSWORD 'new_password1';

\c test_db - - :worker_1_port
ALTER USER "test_role2-needs\!escape"
WITH
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN NOREPLICATION NOBYPASSRLS CONNECTION
LIMIT 5 VALID UNTIL '2024-01-01';


\c regression - - :master_port

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, rolvaliduntil
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY rolname
    ) t
$$);


select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
        FROM pg_auth_members
        WHERE member::regrole::text in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY member::regrole::text
    ) t
$$);

\c test_db - - :master_port

-- Test DROP ROLE
DROP ROLE IF EXISTS test_role1, "test_role2-needs\!escape";

-- Clean up: drop the database

\c regression - - :master_port
set citus.enable_create_database_propagation to on;

DROP DATABASE test_db;

reset citus.enable_create_database_propagation;
