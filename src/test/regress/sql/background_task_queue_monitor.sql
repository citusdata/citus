CREATE SCHEMA background_task_queue_monitor;
SET search_path TO background_task_queue_monitor;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 3536400;

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

CREATE TABLE results (a int);

-- simple job that inserts 1 into results to show that query runs
SELECT a FROM results WHERE a = 1; -- verify result is not in there
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 1 ); $job$) RETURNING task_id \gset
SELECT citus_job_wait(:job_id); -- wait for the job to be finished
SELECT a FROM results WHERE a = 1; -- verify result is there

-- cancel a scheduled job
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job','cancel a scheduled job') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id \gset

SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

-- verify we get an error when waiting for a job to reach a specific status while it is already in a different terminal status
SELECT citus_job_wait(:job_id, desired_status => 'finished');

-- show that the status has been cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message IS NULL) AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- cancel a running job
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job','cancelling a task after it started') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');
SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

-- show that the status has been cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message IS NULL) AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test a failing task becomes runnable in the future again
-- we cannot fully test the backoff strategy currently as it is hard coded to take about 50 minutes.
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'failure test due to division by zero') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT 1/0; $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');
SELECT pg_sleep(.1); -- make sure it has time to error after it started running

SELECT status, pid, retry_count, NOT (message IS NULL) AS has_message, (not_before > now()) AS scheduled_into_the_future FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test cancelling a failed/retrying job
SELECT citus_job_cancel(:job_id);

SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message IS NULL) AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test running two dependant tasks
TRUNCATE TABLE results;
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 5 ); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a * 7; $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a + 13; $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id2, :task_id1);
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id3, :task_id2);
COMMIT;

SELECT citus_job_wait(:job_id); -- wait for the job to be finished
SELECT a FROM results;

-- test running two dependant tasks, with a failing task that we cancel
TRUNCATE TABLE results;
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 5 ); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ SELECT 1/0; $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, status, command) VALUES (:job_id, 'blocked', $job$ UPDATE background_task_queue_monitor.results SET a = a + 13; $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id2, :task_id1);
INSERT INTO pg_dist_background_task_depend (job_id, task_id, depends_on) VALUES (:job_id, :task_id3, :task_id2);
COMMIT;

SELECT citus_job_wait(:job_id, desired_status => 'running'); -- wait for the job to be running, and possibly hitting a failure
SELECT pg_sleep(.1); -- improve chances of hitting the failure

SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id); -- wait for the job to be cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message IS NULL) AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

SET client_min_messages TO WARNING;
DROP SCHEMA background_task_queue_monitor CASCADE;

ALTER SYSTEM RESET citus.background_task_queue_interval;
SELECT pg_reload_conf();
