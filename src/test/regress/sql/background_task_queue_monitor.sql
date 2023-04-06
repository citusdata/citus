CREATE SCHEMA background_task_queue_monitor;
SET search_path TO background_task_queue_monitor;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 3536400;
SET client_min_messages TO ERROR;

-- reset sequence values
ALTER SEQUENCE pg_dist_background_job_job_id_seq RESTART 1450000;
ALTER SEQUENCE pg_dist_background_task_task_id_seq RESTART 1450000;

-- create helper procedure to wait until given task is retried or timeout occurs
CREATE OR REPLACE PROCEDURE citus_wait_task_until_retried(taskid bigint)
LANGUAGE plpgsql
AS $$
DECLARE
    retried boolean := false;
    loop_count int := 0;
BEGIN
    WHILE retried = false and loop_count < 20
    LOOP
        SELECT (retry_count IS NOT NULL) INTO retried FROM pg_dist_background_task WHERE task_id = taskid;
        PERFORM pg_sleep(1);
        loop_count := loop_count + 1;
        COMMIT;
    END LOOP;

    -- timeout if the task is not retried in 20 sec
    IF retried = false
    THEN
        RAISE WARNING 'Timeout while waiting for retrying task:%', taskid;
    END IF;
END;
$$;
--

ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

CREATE TABLE results (a int);

-- TEST1
-- simple job that inserts 1 into results to show that query runs
SELECT a FROM results WHERE a = 1; -- verify result is not in there
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify background execution') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ INSERT INTO background_task_queue_monitor.results VALUES ( 1 ); $job$) RETURNING task_id \gset
SELECT citus_job_wait(:job_id); -- wait for the job to be finished
SELECT a FROM results WHERE a = 1; -- verify result is there

-- TEST2
-- cancel a running job
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job','cancelling a task after it started') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');
SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

-- show that the status has been cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- TEST3
-- test a failing task becomes runnable in the future again
-- we cannot fully test the backoff strategy currently as it is hard coded to take about 50 minutes.
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'failure test due to division by zero') RETURNING job_id \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id, $job$ SELECT 1/0; $job$) RETURNING task_id \gset

SELECT citus_job_wait(:job_id, desired_status => 'running');

CALL citus_wait_task_until_retried(:task_id); -- shows that we increased retry count for task after failure
SELECT status, pid, retry_count, NOT(message = '') AS has_message, (not_before > now()) AS scheduled_into_the_future FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- test cancelling a failed/retrying job
SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id);

SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;


-- TEST4
-- test running two dependent tasks
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

-- TEST5
-- test running two dependent tasks, with a failing task that we cancel
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
CALL citus_wait_task_until_retried(:task_id2); -- shows that we increased retry count for task after failure

SELECT citus_job_cancel(:job_id);
SELECT citus_job_wait(:job_id); -- wait for the job to be cancelled
SELECT state, NOT(started_at IS NULL) AS did_start FROM pg_dist_background_job WHERE job_id = :job_id;
SELECT status, NOT(message = '') AS did_start FROM pg_dist_background_task WHERE job_id = :job_id ORDER BY task_id ASC;

-- TEST6
-- verify that we do not allow parallel task executors more than citus.max_background_task_executors(4 by default)
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id4 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id3 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id3, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id5 \gset
COMMIT;

SELECT citus_task_wait(:task_id1, desired_status => 'running');
SELECT citus_task_wait(:task_id2, desired_status => 'running');
SELECT citus_task_wait(:task_id3, desired_status => 'running');
SELECT citus_task_wait(:task_id4, desired_status => 'running');
SELECT citus_task_wait(:task_id5, desired_status => 'runnable');

SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id; -- show that last task is not running but ready to run(runnable)

SELECT citus_job_cancel(:job_id2); -- when a job with 1 task is cancelled, the last runnable task will be running
SELECT citus_job_wait(:job_id2); -- wait for the job to be cancelled
SELECT citus_job_wait(:job_id3, desired_status => 'running');
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id;  -- show that last task is running

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id3);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id3);
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY job_id, task_id;  -- show that multiple cancels worked

-- TEST7
-- verify that a task, previously not started due to lack of workers, is executed after we increase max worker count
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id4 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify max parallel background execution') RETURNING job_id AS job_id3 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id3, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id5 \gset
COMMIT;

SELECT citus_task_wait(:task_id1, desired_status => 'running');
SELECT citus_task_wait(:task_id2, desired_status => 'running');
SELECT citus_task_wait(:task_id3, desired_status => 'running');
SELECT citus_task_wait(:task_id4, desired_status => 'running');
SELECT citus_task_wait(:task_id5, desired_status => 'runnable');

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id; -- show that last task is not running but ready to run(runnable)

ALTER SYSTEM SET citus.max_background_task_executors TO 5;
SELECT pg_reload_conf(); -- the last runnable task will be running after change
SELECT citus_job_wait(:job_id3, desired_status => 'running');
SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id;  -- show that last task is running

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id2);
SELECT citus_job_cancel(:job_id3);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);
SELECT citus_job_wait(:job_id3);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4, :task_id5)
    ORDER BY task_id; -- show that all tasks are cancelled

-- TEST8
-- verify that upon termination signal, all tasks fail and retry policy sets their status back to runnable
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify termination on monitor') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify termination on monitor') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'running');

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id;


SELECT pid AS monitor_pid FROM pg_stat_activity WHERE application_name ~ 'task queue monitor' \gset
SELECT pg_terminate_backend(:monitor_pid); -- terminate monitor process

CALL citus_wait_task_until_retried(:task_id1); -- shows that we increased retry count for task after failure
CALL citus_wait_task_until_retried(:task_id2); -- shows that we increased retry count for task after failure

SELECT task_id, status, retry_count, message FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are runnable by retry policy after termination signal

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_cancel(:job_id2);
SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are cancelled

-- TEST9
-- verify that upon cancellation signal, all tasks are cancelled
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify cancellation on monitor') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify cancellation on monitor') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'running');

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id;


SELECT pid AS monitor_pid FROM pg_stat_activity WHERE application_name ~ 'task queue monitor' \gset
SELECT pg_cancel_backend(:monitor_pid); -- cancel monitor process

SELECT citus_task_wait(:task_id1, desired_status => 'cancelled'); -- shows that we cancelled the task
SELECT citus_task_wait(:task_id2, desired_status => 'cancelled'); -- shows that we cancelled the task

SELECT citus_job_wait(:job_id1);
SELECT citus_job_wait(:job_id2);

SELECT task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY task_id; -- show that all tasks are cancelled

-- TEST10
-- verify that task is not starved by currently long running task
BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify task execution starvation') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id1, $job$ SELECT pg_sleep(5); $job$) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify task execution starvation') RETURNING job_id AS job_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command) VALUES (:job_id2, $job$ SELECT 1; $job$) RETURNING task_id AS task_id2 \gset
COMMIT;

SELECT citus_job_wait(:job_id1, desired_status => 'running');
SELECT citus_job_wait(:job_id2, desired_status => 'finished');
SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY job_id, task_id;  -- show that last task is finished without starvation
SELECT citus_job_cancel(:job_id1);
SELECT citus_job_wait(:job_id1);

SELECT job_id, task_id, status FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2)
    ORDER BY job_id, task_id;  -- show that task is cancelled

-- TEST11
-- verify that we do not allow parallel task executors involving a particular node
-- more than citus.max_background_task_executors_per_node
-- verify that we can change citus.max_background_task_executors_per_node on the fly
-- tests are done with dummy node ids
-- citus_task_wait calls are used to ensure consistent pg_dist_background_task query
-- output i.e. to avoid flakiness

BEGIN;
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test_job', 'simple test to verify changing max background task executors per node on the fly') RETURNING job_id AS job_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(2); $job$, ARRAY [1, 2]) RETURNING task_id AS task_id1 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(2); $job$, ARRAY [3, 4]) RETURNING task_id AS task_id2 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(4); $job$, ARRAY [1, 2]) RETURNING task_id AS task_id3 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(4); $job$, ARRAY [1, 3]) RETURNING task_id AS task_id4 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(4); $job$, ARRAY [2, 4]) RETURNING task_id AS task_id5 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(7); $job$, ARRAY [1, 2]) RETURNING task_id AS task_id6 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(6); $job$, ARRAY [1, 3]) RETURNING task_id AS task_id7 \gset
INSERT INTO pg_dist_background_task (job_id, command, nodes_involved) VALUES (:job_id1, $job$ SELECT pg_sleep(6); $job$, ARRAY [1, 4]) RETURNING task_id AS task_id8 \gset
COMMIT;

SELECT citus_task_wait(:task_id1, desired_status => 'running');
SELECT citus_task_wait(:task_id2, desired_status => 'running');

SELECT job_id, task_id, status, nodes_involved FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4,
                      :task_id5, :task_id6, :task_id7, :task_id8)
    ORDER BY job_id, task_id; -- show that at most 1 task per node is running

SELECT citus_task_wait(:task_id1, desired_status => 'done');
SELECT citus_task_wait(:task_id2, desired_status => 'done');
-- increase max_background_task_executors_per_node on the fly
ALTER SYSTEM SET citus.max_background_task_executors_per_node = 2;
SELECT pg_reload_conf();

SELECT citus_task_wait(:task_id3, desired_status => 'running');
SELECT citus_task_wait(:task_id4, desired_status => 'running');
SELECT citus_task_wait(:task_id5, desired_status => 'running');

SELECT job_id, task_id, status, nodes_involved FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4,
                      :task_id5, :task_id6, :task_id7, :task_id8)
    ORDER BY job_id, task_id; -- show that at most 2 tasks per node are running

-- increase to 3 max_background_task_executors_per_node on the fly
SELECT citus_task_wait(:task_id3, desired_status => 'done');
SELECT citus_task_wait(:task_id4, desired_status => 'done');
SELECT citus_task_wait(:task_id5, desired_status => 'done');
ALTER SYSTEM SET citus.max_background_task_executors_per_node = 3;
SELECT pg_reload_conf();

SELECT citus_task_wait(:task_id6, desired_status => 'running');
SELECT citus_task_wait(:task_id7, desired_status => 'running');
SELECT citus_task_wait(:task_id8, desired_status => 'running');

SELECT job_id, task_id, status, nodes_involved FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4,
                      :task_id5, :task_id6, :task_id7, :task_id8)
    ORDER BY job_id, task_id; -- show that at most 3 tasks per node are running

ALTER SYSTEM RESET citus.max_background_task_executors_per_node;
SELECT pg_reload_conf();

-- if pg_cancel_backend is called on one of the running task PIDs
-- task doesn't restart because it's not allowed anymore by the limit.
-- node with id 1 can be used only once, unless there are previously running tasks
SELECT pid AS task_id6_pid FROM pg_dist_background_task WHERE task_id IN (:task_id6) \gset
SELECT pg_cancel_backend(:task_id6_pid); -- cancel task_id6 process

-- task goes to only runnable state, not running anymore.
SELECT citus_task_wait(:task_id6, desired_status => 'runnable');

-- show that cancelled task hasn't restarted because limit doesn't allow it
SELECT job_id, task_id, status, nodes_involved FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4,
                      :task_id5, :task_id6, :task_id7, :task_id8)
    ORDER BY job_id, task_id;

SELECT citus_task_wait(:task_id7, desired_status => 'done');
SELECT citus_task_wait(:task_id8, desired_status => 'done');
SELECT citus_task_wait(:task_id6, desired_status => 'running');

-- show that the 6th task has restarted only after both 6 and 7 are done
-- since we have a limit of 1 background task executor per node with id 1
SELECT job_id, task_id, status, nodes_involved FROM pg_dist_background_task
    WHERE task_id IN (:task_id1, :task_id2, :task_id3, :task_id4,
                      :task_id5, :task_id6, :task_id7, :task_id8)
    ORDER BY job_id, task_id;

SELECT citus_job_cancel(:job_id1);
SELECT citus_job_wait(:job_id1);

ALTER SYSTEM RESET citus.max_background_task_executors_per_node;
SELECT pg_reload_conf();

SET client_min_messages TO WARNING;
TRUNCATE pg_dist_background_job CASCADE;
TRUNCATE pg_dist_background_task CASCADE;
TRUNCATE pg_dist_background_task_depend;
DROP SCHEMA background_task_queue_monitor CASCADE;
RESET client_min_messages;

ALTER SYSTEM RESET citus.background_task_queue_interval;
ALTER SYSTEM RESET citus.max_background_task_executors;
SELECT pg_reload_conf();
