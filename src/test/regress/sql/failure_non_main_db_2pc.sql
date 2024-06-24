SELECT citus.mitmproxy('conn.allow()');

CREATE SCHEMA failure_non_main_db_2pc;
SET SEARCH_PATH TO 'failure_non_main_db_2pc';

CREATE DATABASE other_db1;

SELECT citus.mitmproxy('conn.onQuery(query="COMMIT PREPARED").kill()');

\c other_db1

CREATE USER user_1;

\c regression

SELECT citus.mitmproxy('conn.allow()');

SELECT nodeid, result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_1'$$) ORDER BY 1;

SELECT recover_prepared_transactions();

SELECT nodeid, result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_1'$$) ORDER BY 1;


SELECT citus.mitmproxy('conn.onQuery(query="CREATE USER user_2").kill()');

\c other_db1

CREATE USER user_2;

\c regression

SELECT citus.mitmproxy('conn.allow()');

SELECT nodeid, result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_2'$$) ORDER BY 1;

SELECT recover_prepared_transactions();

SELECT nodeid, result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_2'$$) ORDER BY 1;

DROP DATABASE other_db1;
-- user_2 should not exist because the query to create it will fail
-- but let's make sure we try to drop it just in case
DROP USER IF EXISTS user_1, user_2;

SELECT citus_set_coordinator_host('localhost');

\c - - - :worker_1_port

CREATE DATABASE other_db2;

SELECT citus.mitmproxy('conn.onQuery(query="COMMIT PREPARED").kill()');

\c other_db2

CREATE USER user_3;

\c regression

SELECT citus.mitmproxy('conn.allow()');

SELECT result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_3'$$) ORDER BY 1;

SELECT recover_prepared_transactions();

SELECT result FROM run_command_on_all_nodes($$SELECT rolname FROM pg_roles WHERE rolname::TEXT = 'user_3'$$) ORDER BY 1;

DROP DATABASE other_db2;
DROP USER user_3;

\c - - - :master_port

SELECT result FROM  run_command_on_all_nodes($$DELETE FROM pg_dist_node WHERE groupid = 0$$);

DROP SCHEMA failure_non_main_db_2pc;
