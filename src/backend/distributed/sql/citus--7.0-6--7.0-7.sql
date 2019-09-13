/* citus--7.0-6--7.0-7 */

CREATE FUNCTION citus.replace_isolation_tester_func()
RETURNS void AS $$
  DECLARE
    version integer := current_setting('server_version_num');
  BEGIN
    IF version >= 100000 THEN
      ALTER FUNCTION pg_catalog.pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO old_pg_isolation_test_session_is_blocked;
      ALTER FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO pg_isolation_test_session_is_blocked;
    ELSE
      ALTER FUNCTION pg_catalog.pg_blocking_pids(integer)
        RENAME TO old_pg_blocking_pids;
      ALTER FUNCTION pg_catalog.citus_blocking_pids(integer)
        RENAME TO pg_blocking_pids;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.restore_isolation_tester_func()
RETURNS void AS $$
  DECLARE
    version integer := current_setting('server_version_num');
  BEGIN
    IF version >= 100000 THEN
      ALTER FUNCTION pg_catalog.pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO citus_isolation_test_session_is_blocked;
      ALTER FUNCTION pg_catalog.old_pg_isolation_test_session_is_blocked(integer, integer[])
        RENAME TO pg_isolation_test_session_is_blocked;
    ELSE
      ALTER FUNCTION pg_catalog.pg_blocking_pids(integer)
        RENAME TO citus_blocking_pids;
      ALTER FUNCTION pg_catalog.old_pg_blocking_pids(integer)
        RENAME TO pg_blocking_pids;
    END IF;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION citus.refresh_isolation_tester_prepared_statement()
RETURNS void AS $$
  BEGIN
    -- isolation creates a prepared statement using the old function before tests have a
    -- chance to call replace_isolation_tester_func. By calling that prepared statement
    -- with a different search_path we force a re-parse which picks up the new function
    SET search_path TO 'citus';
    EXECUTE 'EXECUTE isolationtester_waiting (0)';
    RESET search_path;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_catalog.citus_blocking_pids(pBlockedPid integer)
RETURNS int4[] AS $$
  DECLARE
    mLocalBlockingPids int4[];
    mRemoteBlockingPids int4[];
    mLocalTransactionNum int8;
  BEGIN
    SELECT pg_catalog.old_pg_blocking_pids(pBlockedPid) INTO mLocalBlockingPids;

    IF (array_length(mLocalBlockingPids, 1) > 0) THEN
      RETURN mLocalBlockingPids;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    SELECT transaction_number INTO mLocalTransactionNum
      FROM get_all_active_transactions() WHERE process_id = pBlockedPid;
    
    SELECT array_agg(process_id) INTO mRemoteBlockingPids FROM (
      WITH activeTransactions AS (
        SELECT process_id, transaction_number FROM get_all_active_transactions()
      ), blockingTransactions AS (
        SELECT blocking_transaction_num AS txn_num FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mLocalTransactionNum
      )
      SELECT activeTransactions.process_id FROM activeTransactions, blockingTransactions
      WHERE activeTransactions.transaction_number = blockingTransactions.txn_num
    ) AS sub;

    RETURN mRemoteBlockingPids;
  END;
$$ LANGUAGE plpgsql;

CREATE FUNCTION pg_catalog.citus_isolation_test_session_is_blocked(pBlockedPid integer, pInterestingPids integer[])
RETURNS boolean AS $$
  DECLARE
    mBlockedTransactionNum int8;
  BEGIN
    IF pg_catalog.old_pg_isolation_test_session_is_blocked(pBlockedPid, pInterestingPids) THEN
      RETURN true;
    END IF;

    -- pg says we're not blocked locally; check whether we're blocked globally.
    SELECT transaction_number INTO mBlockedTransactionNum
      FROM get_all_active_transactions() WHERE process_id = pBlockedPid;

    RETURN EXISTS (
      SELECT 1 FROM dump_global_wait_edges()
        WHERE waiting_transaction_num = mBlockedTransactionNum
    );
  END;
$$ LANGUAGE plpgsql;
