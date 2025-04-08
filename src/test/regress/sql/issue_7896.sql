---------------------------------------------------------------------
-- Regression Test: Simulate zombie replication slot when
-- citus_rebalance_wait() is canceled.
--
-- In the buggy behavior, canceling citus_rebalance_wait()
-- (via a short statement_timeout or Ctrl+C) leaves behind an active logical
-- replication slot on a worker. This, in turn, prevents DROP DATABASE
-- (with FORCE) from succeeding.
--
-- With the fix applied, the underlying rebalance job is canceled,
-- no zombie slot remains.
---------------------------------------------------------------------

---------------------------------------------------------------------
-- 1) Setup the test environment.
---------------------------------------------------------------------
SET citus.next_shard_id TO 17560000;
CREATE SCHEMA issue_7896;
SET search_path TO issue_7896;

---------------------------------------------------------------------
-- 2) Set cluster parameters and initialize environment.
---------------------------------------------------------------------
SET citus.shard_replication_factor TO 2;
SET citus.enable_repartition_joins TO ON;
-- For faster background task processing, set a short background task queue interval.
ALTER SYSTEM SET citus.background_task_queue_interval TO '1s';
SELECT pg_reload_conf();

---------------------------------------------------------------------
-- 3) Create a distributed table.
---------------------------------------------------------------------
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (a int PRIMARY KEY);
SELECT create_distributed_table('t1', 'a', shard_count => 4, colocate_with => 'none');

---------------------------------------------------------------------
-- 4) Insert enough data so that a rebalance has measurable work.
---------------------------------------------------------------------
INSERT INTO t1
  SELECT generate_series(1, 1000000);

---------------------------------------------------------------------
-- 5) Verify that a rebalance on a balanced cluster is a no-op.
---------------------------------------------------------------------
SELECT 1 FROM citus_rebalance_start();
-- Expected: NOTICE "No moves available for rebalancing".
SELECT citus_rebalance_wait();
-- Expected: WARNING "no ongoing rebalance that can be waited on".

---------------------------------------------------------------------
-- 6) Force a shard movement so that a rebalance job is scheduled.
-- Remove and re-add a worker using a parameter placeholder.
---------------------------------------------------------------------
SELECT citus_remove_node('localhost', :worker_2_port);
SELECT citus_add_node('localhost', :worker_2_port);

---------------------------------------------------------------------
-- 7) Start a rebalance job that will do actual work.
---------------------------------------------------------------------
SELECT citus_rebalance_start(
         rebalance_strategy := 'by_disk_size',
         shard_transfer_mode   := 'force_logical'
       );
-- Expected: Notice that moves are scheduled as a background job.

---------------------------------------------------------------------
-- 8) Attempt to wait on the rebalance with a short timeout so that the wait
--    is canceled. The PG_CATCH block in citus_job_wait_internal should then
--    cancel the underlying job (cleaning up temporary replication slots).
---------------------------------------------------------------------
SET statement_timeout = '2s';
DO $$
BEGIN
    BEGIN
        RAISE NOTICE 'Waiting on rebalance with a 2-second timeout...';
        -- Public function citus_rebalance_wait() takes no arguments.
        PERFORM citus_rebalance_wait();
    EXCEPTION
        WHEN query_canceled THEN
            RAISE NOTICE 'Rebalance wait canceled as expected';
            -- Your fix should cancel the underlying rebalance job.
    END;
END;
$$ LANGUAGE plpgsql;
SET statement_timeout = '0';

---------------------------------------------------------------------
-- 9) Cleanup orphaned background resources (if any).
---------------------------------------------------------------------
CALL citus_cleanup_orphaned_resources();

---------------------------------------------------------------------
-- 10) Traverse nodes and check for active replication slots.
--
-- Connect to the coordinator and worker nodes, then query for replication slots.
-- Expected Outcome (with the fix applied): No active replication slots.
---------------------------------------------------------------------
\c - - - :master_port
SELECT * FROM pg_replication_slots;

\c - - - :worker_1_port
SELECT * FROM pg_replication_slots;

\c - - - :worker_2_port
SELECT * FROM pg_replication_slots;


---------------------------------------------------------------------
-- 11) Cleanup: Drop the test schema.
---------------------------------------------------------------------
\c - - - :master_port
SET search_path TO issue_7896;
SET client_min_messages TO WARNING;
DROP SCHEMA IF EXISTS issue_7896 CASCADE;

