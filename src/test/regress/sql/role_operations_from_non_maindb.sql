-- Create a new database
set citus.enable_create_database_propagation to on;
CREATE DATABASE role_operations_test_db;
SET citus.superuser TO 'postgres';
-- Connect to the new database
\c role_operations_test_db
-- Test CREATE ROLE with various options
CREATE ROLE test_role1 WITH LOGIN PASSWORD 'password1';

\c role_operations_test_db - - :worker_1_port
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
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY rolname
    ) t
$$);

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT r.rolname
        FROM pg_dist_object d
        JOIN pg_roles r ON d.objid = r.oid
        WHERE r.rolname IN ('test_role1', 'test_role2-needs\!escape')
        order by r.rolname
    ) t
$$);

\c role_operations_test_db - - :master_port
-- Test ALTER ROLE with various options
ALTER ROLE test_role1 WITH PASSWORD 'new_password1';

\c role_operations_test_db - - :worker_1_port
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
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape')
        ORDER BY rolname
    ) t
$$);

\c role_operations_test_db - - :master_port
-- Test DROP ROLE
DROP ROLE no_such_role; -- fails nicely
DROP ROLE IF EXISTS no_such_role;  -- doesn't fail

CREATE ROLE new_role;
DROP ROLE IF EXISTS no_such_role, new_role; -- doesn't fail
DROP ROLE IF EXISTS test_role1, "test_role2-needs\!escape";

\c regression - - :master_port
--verify that roles and dist_object are dropped
select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape','new_role','no_such_role')
        ORDER BY rolname
    ) t
$$);

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT r.rolname
        FROM pg_roles r
        WHERE r.rolname IN ('test_role1', 'test_role2-needs\!escape','new_role','no_such_role')
        order by r.rolname
    ) t
$$);

SELECT result FROM run_command_on_all_nodes($$
  SELECT count(*) leaked_pg_dist_object_records_for_roles
  FROM pg_dist_object LEFT JOIN pg_authid ON (objid = oid)
  WHERE classid = 1260 AND oid IS NULL
$$);

-- Clean up: drop the database
set citus.enable_create_database_propagation to on;
DROP DATABASE role_operations_test_db;
reset citus.enable_create_database_propagation;
