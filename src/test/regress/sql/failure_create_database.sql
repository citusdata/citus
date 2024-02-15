SET citus.enable_create_database_propagation TO ON;
SET client_min_messages TO WARNING;

-- cleanup any orphaned resources from previous runs
SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();

SET citus.next_operation_id TO 4000;

ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SELECT citus.mitmproxy('conn.kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^CREATE DATABASE").cancel(' || pg_backend_pid() || ')');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^ALTER DATABASE").cancel(' || pg_backend_pid() || ')');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^PREPARE TRANSACTION").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onQuery(query="^COMMIT PREPARED").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

-- not call citus_cleanup_orphaned_resources() but recover the prepared transactions this time
SELECT recover_prepared_transactions();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

DROP DATABASE db1;

SELECT citus.mitmproxy('conn.onQuery(query="^SELECT citus_internal.acquire_citus_advisory_object_class_lock").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;

SELECT citus.mitmproxy('conn.onParse(query="^WITH distributed_object_data").kill()');
CREATE DATABASE db1;
SELECT citus.mitmproxy('conn.allow()');

SELECT array_agg(DISTINCT result ORDER BY result) AS temp_databases_on_nodes FROM run_command_on_all_nodes($$SELECT datname FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$) WHERE result != '';
CALL citus_cleanup_orphaned_resources();
SELECT bool_and(result::boolean) AS no_temp_databases_on_any_nodes FROM run_command_on_all_nodes($$SELECT COUNT(*)=0 FROM pg_database WHERE datname LIKE 'citus_temp_database_%'$$);
SELECT * FROM public.check_database_on_all_nodes($$db1$$) ORDER BY node_type, result;
