CREATE FUNCTION citus_internal.restore_isolation_tester_func_skip_self_local_blocks()
RETURNS void AS $$
    BEGIN
        ALTER FUNCTION pg_catalog.pg_isolation_test_session_is_blocked(integer, integer[])
            RENAME TO citus_isolation_test_session_is_blocked_skip_self_local_blocks;
        ALTER FUNCTION pg_catalog.old_pg_isolation_test_session_is_blocked(integer, integer[])
            RENAME TO pg_isolation_test_session_is_blocked;
    END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION citus_internal.restore_isolation_tester_func_skip_self_local_blocks() FROM PUBLIC;
