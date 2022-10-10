CREATE OR REPLACE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedGlobalPid int8;
    workerProcessId integer := current_setting('citus.isolation_test_session_remote_process_id');
    coordinatorProcessId integer := current_setting('citus.isolation_test_session_process_id');
    r record;
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

    RAISE WARNING E'DETECTING BLOCKS FOR %', mBlockedGlobalPid;
    FOR r IN select p.blocking_global_pid, p.blocking_pid, a1.application_name blocking_app, a1.query blocking_query, p.waiting_global_pid, p.waiting_pid, a2.application_name waiting_app, a2.query waiting_query from citus_internal_global_blocked_processes() p join pg_stat_activity a1 on p.blocking_pid = a1.pid join  pg_stat_activity a2 on p.waiting_pid = a2.pid
    LOOP
        RAISE WARNING E'GPID: % % % %\nBLOCKS\nGPID: % % % %', r.blocking_global_pid, r.blocking_pid, r.blocking_app, r.blocking_query, r.waiting_global_pid, r.waiting_pid, r.waiting_app, r.waiting_query;
    END LOOP;

    IF current_setting('citus.isolation_test_check_all_blocks') = 'off' THEN
      RETURN EXISTS (
        SELECT 1 FROM citus_internal_global_blocked_processes()
          WHERE waiting_global_pid = mBlockedGlobalPid
          AND blocking_global_pid in (
              SELECT citus_backend_gpid(pid) FROM unnest(pInterestingPids) pid
          )
      );
    ELSE
      RETURN EXISTS (
        SELECT 1 FROM citus_internal_global_blocked_processes()
          WHERE waiting_global_pid = mBlockedGlobalPid
      );
    END IF;
  END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION citus_isolation_test_session_is_blocked(integer,integer[]) FROM PUBLIC;
