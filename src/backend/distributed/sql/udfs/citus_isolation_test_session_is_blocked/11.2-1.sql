CREATE OR REPLACE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedGlobalPid int8;
    workerProcessId integer := current_setting('citus.isolation_test_session_remote_process_id');
    coordinatorProcessId integer := current_setting('citus.isolation_test_session_process_id');
  BEGIN
    IF pg_catalog.old_pg_isolation_test_session_is_blocked(pBlockedPid, pInterestingPids) THEN
      RETURN true;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    -- Note that worker process may be blocked or waiting for a lock. So we need to
    -- get transaction number for both of them. Following IF provides the transaction
    -- number when the worker process waiting for other session.
    IF EXISTS (SELECT 1 FROM get_global_active_transactions()
               WHERE process_id = workerProcessId AND pBlockedPid = coordinatorProcessId) THEN
      SELECT global_pid INTO mBlockedGlobalPid FROM get_global_active_transactions()
      WHERE process_id = workerProcessId AND pBlockedPid = coordinatorProcessId;
    ELSE
      -- Check whether transactions initiated from the coordinator get locked
      SELECT global_pid INTO mBlockedGlobalPid
        FROM get_all_active_transactions() WHERE process_id = pBlockedPid;
    END IF;

    -- We convert the blocking_global_pid to a regular pid and only look at
    -- blocks caused by the interesting pids, or the workerProcessPid. If we
    -- don't do that we might find unrelated blocks caused by some random
    -- other processes that are not involved in this isolation test. Because we
    -- run our isolation tests on a single physical machine, the PID part of
    -- the GPID is known to be unique within the whole cluster.
    RETURN EXISTS (
      SELECT 1 FROM citus_internal_global_blocked_processes()
        WHERE waiting_global_pid = mBlockedGlobalPid
        AND (
          citus_pid_for_gpid(blocking_global_pid) in (
              select * from unnest(pInterestingPids)
          )
          OR citus_pid_for_gpid(blocking_global_pid) = workerProcessId
        )
    );
  END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION citus_isolation_test_session_is_blocked(integer,integer[]) FROM PUBLIC;
