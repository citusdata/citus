DROP FUNCTION pg_catalog.citus_blocking_pids;
CREATE FUNCTION pg_catalog.citus_blocking_pids(pBlockedPid integer)
RETURNS int4[] AS $$
  DECLARE
    mLocalBlockingPids int4[];
    mRemoteBlockingPids int4[];
    mLocalGlobalPid int8;
  BEGIN
    SELECT pg_catalog.old_pg_blocking_pids(pBlockedPid) INTO mLocalBlockingPids;

    IF (array_length(mLocalBlockingPids, 1) > 0) THEN
      RETURN mLocalBlockingPids;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    SELECT global_pid INTO mLocalGlobalPid
      FROM get_all_active_transactions() WHERE process_id = pBlockedPid;

    SELECT array_agg(global_pid) INTO mRemoteBlockingPids FROM (
      WITH activeTransactions AS (
        SELECT global_pid FROM get_all_active_transactions()
      ), blockingTransactions AS (
        SELECT blocking_global_pid FROM citus_internal_global_blocked_processes()
        WHERE waiting_global_pid = mLocalGlobalPid
      )
      SELECT activeTransactions.global_pid FROM activeTransactions, blockingTransactions
      WHERE activeTransactions.global_pid = blockingTransactions.blocking_global_pid
    ) AS sub;

    RETURN mRemoteBlockingPids;
  END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION citus_blocking_pids(integer) FROM PUBLIC;
