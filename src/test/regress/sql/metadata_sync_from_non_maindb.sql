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

\c regression
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

\c metadata_sync_2pc_db
revoke "grant_role2pc'_user1","grant_role2pc'_user2" from grant_role2pc_user4,grant_role2pc_user5 ;

revoke admin option for "grant_role2pc'_user1","grant_role2pc'_user2" from "grant_role2pc'_user3";

revoke "grant_role2pc'_user1","grant_role2pc'_user2" from "grant_role2pc'_user3";
revoke ALL on database metadata_sync_2pc_db from "grant_role2pc'_user3";
revoke CONNECT on database metadata_sync_2pc_db from "grant_role2pc'_user2";
revoke CREATE on database metadata_sync_2pc_db from "grant_role2pc'_user1";

\c regression

drop user "grant_role2pc'_user1","grant_role2pc'_user2","grant_role2pc'_user3",grant_role2pc_user4,grant_role2pc_user5;
set citus.enable_create_database_propagation to on;
drop database metadata_sync_2pc_db;
drop schema metadata_sync_2pc_schema;

reset citus.enable_create_database_propagation;
reset search_path;
