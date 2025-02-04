SET citus.enable_create_database_propagation TO ON;
SET client_min_messages TO WARNING;

SELECT 1 FROM citus_add_node('localhost', :master_port, 0);

CREATE FUNCTION get_temp_databases_on_nodes()
RETURNS TEXT AS $func$
  SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
$func$
LANGUAGE sql;

CREATE FUNCTION count_db_cleanup_records()
RETURNS TABLE(object_name TEXT, count INTEGER) AS $func$
  SELECT object_name, COUNT(*) FROM pg_dist_cleanup WHERE object_name LIKE 'citus_temp_database_%' GROUP BY object_name;
$func$
LANGUAGE sql;

CREATE FUNCTION ensure_no_temp_databases_on_any_nodes()
RETURNS BOOLEAN AS $func$
  SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
$func$
LANGUAGE sql;

-- cleanup any orphaned resources from previous runs
CALL citus_cleanup_orphaned_resources();

SET citus.next_operation_id TO 4000;

ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SELECT citus.mitmproxy('conn.kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^CREATE DATABASE").cancel(' || pg_backend_pid() || ')');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^ALTER DATABASE").cancel(' || pg_backend_pid() || ')');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

-- not call citus_cleanup_orphaned_resources() but recover the prepared transactions this time
SELECT 1 FROM recover_prepared_transactions();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

DROP DATABASE db1;

-- after recovering the prepared transactions, cleanup records should also be removed
SELECT * FROM count_db_cleanup_records();

SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_internal.acquire_citus_advisory_object_class_lock").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onParse(query="^WITH distributed_object_data").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT get_temp_databases_on_nodes();
SELECT * FROM count_db_cleanup_records();
CALL citus_cleanup_orphaned_resources();
SELECT ensure_no_temp_databases_on_any_nodes();
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

CREATE DATABASE db1;

-- show that a successful database creation doesn't leave any pg_dist_cleanup records behind
SELECT * FROM count_db_cleanup_records();

DROP DATABASE db1;

DROP FUNCTION get_temp_databases_on_nodes();
DROP FUNCTION ensure_no_temp_databases_on_any_nodes();
DROP FUNCTION count_db_cleanup_records();

SELECT 1 FROM citus_remove_node('localhost', :master_port);
