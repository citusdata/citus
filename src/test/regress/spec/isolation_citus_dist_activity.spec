setup
{
    SET citus.shard_replication_factor TO 1;
    SET citus.shard_count TO 4;
    select setval('pg_dist_shardid_seq', GREATEST(1300000, nextval('pg_dist_shardid_seq')));

    CREATE TABLE test_table(column1 int, column2 int);
    SELECT create_distributed_table('test_table', 'column1');
}

teardown
{
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
	SELECT query, query_hostname, query_hostport, master_query_host_name, master_query_host_port, state, wait_event_type, wait_event, usename, datname FROM citus_dist_stat_activity WHERE query NOT ILIKE '%pg_prepared_xacts%' AND query NOT ILIKE '%COMMIT%' ORDER BY query DESC;

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
	SELECT query, query_hostname, query_hostport, master_query_host_name, master_query_host_port, state, wait_event_type, wait_event, usename, datname FROM citus_worker_stat_activity WHERE query NOT ILIKE '%pg_prepared_xacts%' AND query NOT ILIKE '%COMMIT%' ORDER BY query DESC;
}

// we prefer to sleep before "s2-view-dist" so that we can ensure
// the "wait_event" in the output doesn't change randomly (e.g., NULL to CliendRead etc.)
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-alter-table" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-insert" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-select" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
permutation "s1-cache-connections" "s1-begin" "s2-begin" "s3-begin" "s1-select-router" "s2-sleep" "s2-view-dist" "s3-view-worker" "s2-rollback" "s1-commit" "s3-rollback"
