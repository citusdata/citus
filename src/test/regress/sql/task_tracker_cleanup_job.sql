--
-- TASK_TRACKER_CLEANUP_JOB
--


SET citus.next_shard_id TO 1060000;


\set JobId 401010
\set CompletedTaskId 801107
\set RunningTaskId 801108

-- Test worker_cleanup_job_schema_cache
SELECT * FROM task_tracker_assign_task(2, 2, '');
SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'pg_merge_job_0002';
SELECT worker_cleanup_job_schema_cache();
SELECT count(*) FROM pg_catalog.pg_namespace WHERE nspname = 'pg_merge_job_0002';

-- We assign two tasks to the task tracker. The first task should complete and
-- the second task should continue to keep running.

SELECT task_tracker_assign_task(:JobId, :CompletedTaskId,
				'COPY (SELECT * FROM lineitem) TO '
				'''base/pgsql_job_cache/job_401010/task_801107''');

SELECT task_tracker_assign_task(:JobId, :RunningTaskId,
				'SELECT pg_sleep(100)');

SELECT pg_sleep(2.0);

SELECT task_tracker_task_status(:JobId, :CompletedTaskId);
SELECT task_tracker_task_status(:JobId, :RunningTaskId);

SELECT isdir FROM pg_stat_file('base/pgsql_job_cache/job_401010/task_801107');
SELECT isdir FROM pg_stat_file('base/pgsql_job_cache/job_401010');

-- We now clean up all tasks for this job id. As a result, shared hash entries,
-- files, and connections associated with these tasks should all be cleaned up.

SELECT task_tracker_cleanup_job(:JobId);

SELECT pg_sleep(1.0);

SELECT task_tracker_task_status(:JobId, :CompletedTaskId);
SELECT task_tracker_task_status(:JobId, :RunningTaskId);

SELECT isdir FROM pg_stat_file('base/pgsql_job_cache/job_401010/task_801107');
SELECT isdir FROM pg_stat_file('base/pgsql_job_cache/job_401010');

-- Also clean up worker_cleanup_job_schema_cache job
SELECT task_tracker_cleanup_job(2);
