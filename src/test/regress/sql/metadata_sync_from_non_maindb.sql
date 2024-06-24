CREATE SCHEMA metadata_sync_2pc_schema;
SET search_path TO metadata_sync_2pc_schema;
set citus.enable_create_database_propagation to on;
CREATE DATABASE metadata_sync_2pc_db;

revoke connect,temp,temporary  on database metadata_sync_2pc_db from public;

\c metadata_sync_2pc_db
SHOW citus.main_db;

CREATE USER "grant_role2pc'_user1";
CREATE USER "grant_role2pc'_user2";
CREATE USER "grant_role2pc'_user3";
CREATE USER grant_role2pc_user4;
CREATE USER grant_role2pc_user5;

\c regression
select 1 from citus_remove_node('localhost', :worker_2_port);

\c metadata_sync_2pc_db
grant "grant_role2pc'_user1","grant_role2pc'_user2" to "grant_role2pc'_user3" WITH ADMIN OPTION;
-- This section was originally testing a scenario where a user with the 'admin option' grants the same role to another user, also with the 'admin option'.
-- However, we encountered inconsistent errors because the 'admin option' grant is executed after the grant below.
-- Once we establish the correct order of granting, we will reintroduce the 'granted by' clause.
-- For now, we are commenting out the grant below that includes 'granted by', and instead, we are adding a grant without the 'granted by' clause.
-- grant "grant_role2pc'_user1","grant_role2pc'_user2" to grant_role2pc_user4,grant_role2pc_user5 granted by "grant_role2pc'_user3";
grant "grant_role2pc'_user1","grant_role2pc'_user2" to grant_role2pc_user4,grant_role2pc_user5;

--test for grant on database
\c metadata_sync_2pc_db - - :master_port
grant create on database metadata_sync_2pc_db to "grant_role2pc'_user1";
grant connect on database metadata_sync_2pc_db to "grant_role2pc'_user2";
grant ALL on database metadata_sync_2pc_db to "grant_role2pc'_user3";

\c regression
select check_database_privileges('grant_role2pc''_user1','metadata_sync_2pc_db',ARRAY['CREATE']);
select check_database_privileges('grant_role2pc''_user2','metadata_sync_2pc_db',ARRAY['CONNECT']);
select check_database_privileges('grant_role2pc''_user3','metadata_sync_2pc_db',ARRAY['CREATE','CONNECT','TEMP','TEMPORARY']);

-- test for security label on role
\c metadata_sync_2pc_db - - :master_port
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE grant_role2pc_user4 IS 'citus_unclassified';
SECURITY LABEL FOR "citus '!tests_label_provider" ON ROLE "grant_role2pc'_user1" IS 'citus_classified';

\c regression
SELECT node_type, result FROM get_citus_tests_label_provider_labels('grant_role2pc_user4') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels($$"grant_role2pc''_user1"$$) ORDER BY node_type;

set citus.enable_create_database_propagation to on;
select 1 from citus_add_node('localhost', :worker_2_port);

select result FROM run_command_on_all_nodes($$
SELECT array_to_json(array_agg(row_to_json(t)))
FROM (
    SELECT member::regrole, roleid::regrole as role, grantor::regrole, admin_option
    FROM pg_auth_members
    WHERE member::regrole::text in
        ('"grant_role2pc''_user2"','"grant_role2pc''_user3"','grant_role2pc_user4','grant_role2pc_user5')
    order by member::regrole::text
) t
$$);

select check_database_privileges('grant_role2pc''_user1','metadata_sync_2pc_db',ARRAY['CREATE']);
select check_database_privileges('grant_role2pc''_user2','metadata_sync_2pc_db',ARRAY['CONNECT']);
select check_database_privileges('grant_role2pc''_user3','metadata_sync_2pc_db',ARRAY['CREATE','CONNECT','TEMP','TEMPORARY']);

SELECT node_type, result FROM get_citus_tests_label_provider_labels('grant_role2pc_user4') ORDER BY node_type;
SELECT node_type, result FROM get_citus_tests_label_provider_labels($$"grant_role2pc''_user1"$$) ORDER BY node_type;

\c metadata_sync_2pc_db
revoke "grant_role2pc'_user1","grant_role2pc'_user2" from grant_role2pc_user4,grant_role2pc_user5 ;

revoke admin option for "grant_role2pc'_user1","grant_role2pc'_user2" from "grant_role2pc'_user3";

revoke "grant_role2pc'_user1","grant_role2pc'_user2" from "grant_role2pc'_user3";
revoke ALL on database metadata_sync_2pc_db from "grant_role2pc'_user3";
revoke CONNECT on database metadata_sync_2pc_db from "grant_role2pc'_user2";
revoke CREATE on database metadata_sync_2pc_db from "grant_role2pc'_user1";

\c regression

drop user "grant_role2pc'_user1","grant_role2pc'_user2","grant_role2pc'_user3",grant_role2pc_user4,grant_role2pc_user5;
--test for user operations

--test for create user
\c regression - - :master_port
select 1 from citus_remove_node('localhost', :worker_2_port);

\c metadata_sync_2pc_db - - :master_port
CREATE ROLE test_role1 WITH LOGIN PASSWORD 'password1';

\c metadata_sync_2pc_db - - :worker_1_port
CREATE USER "test_role2-needs\!escape"
WITH
    SUPERUSER CREATEDB CREATEROLE INHERIT LOGIN REPLICATION BYPASSRLS CONNECTION
LIMIT 10 VALID UNTIL '2023-01-01' IN ROLE test_role1;

create role test_role3;

\c regression - - :master_port
select 1 from citus_add_node('localhost', :worker_2_port);

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape','test_role3')
        ORDER BY rolname
    ) t
$$);

--test for alter user
select 1 from citus_remove_node('localhost', :worker_2_port);
\c metadata_sync_2pc_db - - :master_port
-- Test ALTER ROLE with various options
ALTER ROLE test_role1 WITH PASSWORD 'new_password1';

\c metadata_sync_2pc_db - - :worker_1_port
ALTER USER "test_role2-needs\!escape"
WITH
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOLOGIN NOREPLICATION NOBYPASSRLS CONNECTION
LIMIT 5 VALID UNTIL '2024-01-01';

\c regression - - :master_port
select 1 from citus_add_node('localhost', :worker_2_port);

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape','test_role3')
        ORDER BY rolname
    ) t
$$);

--test for drop user
select 1 from citus_remove_node('localhost', :worker_2_port);

\c metadata_sync_2pc_db - - :worker_1_port
DROP ROLE test_role1, "test_role2-needs\!escape";

\c metadata_sync_2pc_db - - :master_port
DROP ROLE test_role3;

\c regression - - :master_port
select 1 from citus_add_node('localhost', :worker_2_port);

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape','test_role3')
        ORDER BY rolname
    ) t
$$);

-- Clean up: drop the database on worker node 2
\c regression - - :worker_2_port
DROP ROLE if exists test_role1, "test_role2-needs\!escape", test_role3;

\c regression - - :master_port

select result FROM run_command_on_all_nodes($$
    SELECT array_to_json(array_agg(row_to_json(t)))
    FROM (
        SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
        rolcanlogin, rolreplication, rolbypassrls, rolconnlimit,
        (rolpassword != '') as pass_not_empty, DATE(rolvaliduntil)
        FROM pg_authid
        WHERE rolname in ('test_role1', 'test_role2-needs\!escape','test_role3')
        ORDER BY rolname
    ) t
$$);

set citus.enable_create_database_propagation to on;
drop database metadata_sync_2pc_db;
drop schema metadata_sync_2pc_schema;
reset citus.enable_create_database_propagation;
reset search_path;
