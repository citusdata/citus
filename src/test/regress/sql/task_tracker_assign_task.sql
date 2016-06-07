--
-- TASK_TRACKER_ASSIGN_TASK
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1050000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1050000;


\set JobId 401010
\set SimpleTaskId 101101
\set RecoverableTaskId 801102

\set SimpleTaskTable lineitem_simple_task
\set BadQueryString '\'SELECT COUNT(*) FROM bad_table_name\''
\set GoodQueryString '\'SELECT COUNT(*) FROM lineitem\''
\set SelectAll 'SELECT *'

-- We assign two tasks to the task tracker. The first task simply executes. The
-- recoverable task on the other hand repeatedly fails, and we sleep until the
-- task tracker stops retrying the recoverable task.

SELECT task_tracker_assign_task(:JobId, :SimpleTaskId,
				'COPY (SELECT * FROM lineitem) TO '
				'''base/pgsql_job_cache/job_401010/task_101101''');

SELECT task_tracker_assign_task(:JobId, :RecoverableTaskId, :BadQueryString);

-- After assigning the two tasks, we wait for them to make progress. Note that
-- these tasks get scheduled and run asynchronously, so if the sleep interval is
-- not enough, the regression tests may fail on an overloaded box.

SELECT pg_sleep(3.0);

SELECT task_tracker_task_status(:JobId, :SimpleTaskId);
SELECT task_tracker_task_status(:JobId, :RecoverableTaskId);

COPY :SimpleTaskTable FROM 'base/pgsql_job_cache/job_401010/task_101101';

SELECT COUNT(*) FROM :SimpleTaskTable;

SELECT COUNT(*) AS diff_lhs FROM ( :SelectAll FROM :SimpleTaskTable EXCEPT ALL
       		   	    	   :SelectAll FROM lineitem ) diff;
SELECT COUNT(*) As diff_rhs FROM ( :SelectAll FROM lineitem EXCEPT ALL
       		   	    	   :SelectAll FROM :SimpleTaskTable ) diff;

-- We now reassign the recoverable task with a good query string. This updates
-- the task's query string, and reschedules the updated task for execution.

SELECT task_tracker_assign_task(:JobId, :RecoverableTaskId, :GoodQueryString);

SELECT pg_sleep(2.0);

SELECT task_tracker_task_status(:JobId, :RecoverableTaskId);
