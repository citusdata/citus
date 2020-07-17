-- citus--9.4-1--9.5-1

-- bump version to 9.5-1

DROP FUNCTION task_tracker_assign_task(bigint, integer, text);
DROP FUNCTION task_tracker_task_status(bigint, integer);
DROP FUNCTION task_tracker_cleanup_job(bigint);
DROP FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text);
DROP FUNCTION worker_execute_sql_task(bigint, integer, text, bool);
