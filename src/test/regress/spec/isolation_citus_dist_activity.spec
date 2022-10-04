setup
{
    -- revert back to pg_isolation_test_session_is_blocked until the tests are fixed
    SELECT citus_internal.restore_isolation_tester_func();

    CREATE OR REPLACE FUNCTION test_assign_global_pid()
        RETURNS void
        LANGUAGE C STRICT
    AS 'citus', $$test_assign_global_pid$$;
    SET citus.shard_replication_factor TO 1;
    SET citus.shard_count TO 4;
    ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1300001;

    CREATE TABLE test_table(column1 int, column2 int);
    SELECT create_distributed_table('test_table', 'column1');
}

teardown
{
    -- replace pg_isolation_test_session_is_blocked so that next tests are run with Citus implementation
    SELECT citus_internal.replace_isolation_tester_func();

    DROP TABLE test_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-cache-connections"
{
    SET citus.max_cached_conns_per_worker TO 4;
    SET citus.force_max_query_parallelization TO on;
    UPDATE test_table SET column2 = 0;
}

step "s1-alter-table"
{
    ALTER TABLE test_table ADD COLUMN x INT;
}

step "s1-select"
{
   SELECT count(*) FROM test_table;
}

step "s1-select-router"
{
   SELECT count(*) FROM test_table WHERE column1 = 55;
}

step "s1-insert"
{
 	INSERT INTO test_table VALUES (100, 100);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
	BEGIN;
}

step "s2-rollback"
{
	ROLLBACK;
}

step "s2-sleep"
{
	SELECT pg_sleep(0.5);
}

step "s2-view-dist"
{
	SELECT query, citus_nodename_for_nodeid(citus_nodeid_for_gpid(global_pid)), citus_nodeport_for_nodeid(citus_nodeid_for_gpid(global_pid)), state, wait_event_type, wait_event, usename, datname FROM citus_dist_stat_activity WHERE query NOT ILIKE ALL(VALUES('%pg_prepared_xacts%'), ('%COMMIT%'), ('%BEGIN%'), ('%pg_catalog.pg_isolation_test_session_is_blocked%'), ('%citus_add_node%'), ('%csa_from_one_node%')) AND backend_type = 'client backend' ORDER BY query DESC;
}

session "s3"

step "s3-begin"
{
	BEGIN;
}

step "s3-rollback"
{
	ROLLBACK;
}

step "s3-view-worker"
{
	SELECT query, citus_nodename_for_nodeid(citus_nodeid_for_gpid(global_pid)), citus_nodeport_for_nodeid(citus_nodeid_for_gpid(global_pid)), state, wait_event_type, wait_event, usename, datname FROM citus_stat_activity WHERE query NOT ILIKE ALL(VALUES('%pg_prepared_xacts%'), ('%COMMIT%'), ('%csa_from_one_node%')) AND is_worker_query = true AND backend_type = 'client backend' ORDER BY query DESC;
}

// we prefer to sleep before "s2-view-dist" so that we can ensure
// the "wait_event" in the output doesn't change randomly (e.g., NULL to CliendRead etc.)
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-alter-table" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-insert" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-select" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-select-router" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
