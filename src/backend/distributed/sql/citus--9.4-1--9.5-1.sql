-- citus--9.4-1--9.5-1

-- bump version to 9.5-1
#include "udfs/undistribute_table/9.5-1.sql"
#include "udfs/create_citus_local_table/9.5-1.sql"

SET search_path = 'pg_catalog';

DROP FUNCTION task_tracker_assign_task(bigint, integer, text);
DROP FUNCTION task_tracker_task_status(bigint, integer);
DROP FUNCTION task_tracker_cleanup_job(bigint);
DROP FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text);
DROP FUNCTION worker_execute_sql_task(bigint, integer, text, bool);
DROP TRIGGER dist_authinfo_task_tracker_cache_invalidate ON pg_catalog.pg_dist_authinfo;
DROP TRIGGER dist_poolinfo_task_tracker_cache_invalidate ON pg_catalog.pg_dist_poolinfo;
DROP FUNCTION task_tracker_conninfo_cache_invalidate();

RESET search_path;
