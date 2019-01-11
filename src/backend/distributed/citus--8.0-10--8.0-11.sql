/* citus--8.0-10--8.0-11 */
SET search_path = 'pg_catalog';

-- Deprecated functions
DROP FUNCTION IF EXISTS worker_hash_partition_table(bigint,integer,text,text,oid,integer);
DROP FUNCTION IF EXISTS worker_foreign_file_path(text);
DROP FUNCTION IF EXISTS worker_find_block_local_path(bigint,text[]);
DROP FUNCTION IF EXISTS worker_fetch_query_results_file(bigint,integer,integer,text,integer);
DROP FUNCTION IF EXISTS master_drop_distributed_table_metadata(regclass,text,text);

-- Testing functions
REVOKE ALL ON FUNCTION citus_blocking_pids(integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION citus_isolation_test_session_is_blocked(integer,integer[]) FROM PUBLIC;

-- Maintenance function
REVOKE ALL ON FUNCTION worker_cleanup_job_schema_cache() FROM PUBLIC;
REVOKE ALL ON FUNCTION recover_prepared_transactions() FROM PUBLIC;
REVOKE ALL ON FUNCTION check_distributed_deadlocks() FROM PUBLIC;

RESET search_path;
