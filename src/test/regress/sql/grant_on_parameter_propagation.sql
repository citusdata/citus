--
-- PG15
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 15 AS server_version_ge_15
\gset
\if :server_version_ge_15
\else
\q
\endif

create user grant_param_user1;
create user grant_param_user2;
create user grant_param_user3;
create user grant_param_user4;
create user "grant_param_user5-\!";


--test the grant command with all options
SET citus.log_remote_commands to on;
SET citus.grep_remote_commands = '%GRANT%';
GRANT SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers TO grant_param_user1,grant_param_user2,"grant_param_user5-\!" WITH GRANT OPTION GRANTED BY CURRENT_USER;

RESET citus.log_remote_commands;
SELECT check_parameter_privileges(ARRAY['grant_param_user1','grant_param_user2','grant_param_user5-\!'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);

--test the grant command admin option using grant_param_user1 with granted by
set role grant_param_user1;
SET citus.log_remote_commands to on;
GRANT ALL ON PARAMETER max_connections,shared_buffers TO grant_param_user3 GRANTED BY grant_param_user1;
SELECT check_parameter_privileges(ARRAY['grant_param_user3'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);

reset role;

--test the revoke command grant option with all options
REVOKE GRANT OPTION FOR SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers FROM grant_param_user1,grant_param_user2,"grant_param_user5-\!" cascade;

--test if the admin option removed for the revoked user. Need to get error
SET ROLE "grant_param_user5-\!";
GRANT SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers TO grant_param_user3 GRANTED BY "grant_param_user5-\!";

SELECT check_parameter_privileges(ARRAY['grant_param_user3'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);

RESET ROLE;

--test the revoke command
REVOKE SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers FROM grant_param_user1,grant_param_user2,grant_param_user3,"grant_param_user5-\!";

RESET citus.log_remote_commands;

SELECT check_parameter_privileges(ARRAY['grant_param_user1','grant_param_user2','grant_param_user3'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);


--test with single permission and single user
GRANT ALTER SYSTEM ON PARAMETER max_connections,shared_buffers TO grant_param_user3;

SELECT check_parameter_privileges(ARRAY['grant_param_user4'],ARRAY['max_connections','shared_buffers'], ARRAY['ALTER SYSTEM']);

--test metadata_sync

SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);
GRANT SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers TO grant_param_user3,"grant_param_user5-\!" WITH GRANT OPTION GRANTED BY CURRENT_USER;

SELECT check_parameter_privileges(ARRAY['grant_param_user3','grant_param_user5-\!'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);

SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT check_parameter_privileges(ARRAY['grant_param_user3','grant_param_user5-\!'],ARRAY['max_connections','shared_buffers'], ARRAY['SET','ALTER SYSTEM']);

REVOKE SET,ALTER SYSTEM ON PARAMETER max_connections,shared_buffers FROM grant_param_user3,"grant_param_user5-\!" cascade;


--clean all resources
DROP USER grant_param_user1;
DROP USER grant_param_user2;
DROP USER grant_param_user3;
DROP USER grant_param_user4;
DROP USER "grant_param_user5-\!";

reset  citus.log_remote_commands;
reset citus.grep_remote_commands;
