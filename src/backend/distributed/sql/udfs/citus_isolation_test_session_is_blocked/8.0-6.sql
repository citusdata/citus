CREATE OR REPLACE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedTransactionNum int8;
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
    IF EXISTS (SELECT transaction_number FROM get_global_active_transactions()
               WHERE process_id = workerProcessId AND pBlockedPid = coordinatorProcessId) THEN
      SELECT transaction_number INTO mBlockedTransactionNum FROM get_global_active_transactions()
      WHERE process_id = workerProcessId AND pBlockedPid = coordinatorProcessId;
    ELSE
      -- Check whether transactions initiated from the coordinator get locked
      SELECT transaction_number INTO mBlockedTransactionNum
        FROM get_all_active_transactions() WHERE process_id = pBlockedPid;
    END IF;

    RETURN EXISTS (
      SELECT 1 FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mBlockedTransactionNum
    );

  END;
$$ LANGUAGE plpgsql;
