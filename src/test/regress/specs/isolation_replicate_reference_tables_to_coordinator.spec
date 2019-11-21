setup
{
  SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();

  SELECT master_add_node('localhost', 57636);

  CREATE TABLE ref_table(a int primary key);
  SELECT create_reference_table('ref_table');
  INSERT INTO ref_table VALUES (1), (3), (5), (7);

  CREATE TABLE dist_table(a int, b int);
  SELECT create_distributed_table('dist_table', 'a');
}

teardown
{
  SELECT citus_internal.restore_isolation_tester_func();
  DROP TABLE ref_table, dist_table;
  SELECT master_remove_node('localhost', 57636);
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-end"
{
    END;
}

step "s1-update-dist-table"
{
    update dist_table set b = 2 where a = 1;
}

step "s1-update-ref-table"
{
    update ref_table set a = a + 1;
}

step "s1-lock-ref-table-placement-on-coordinator"
{
    DO $$
      DECLARE refshardid int;
      BEGIN
        SELECT shardid INTO refshardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass;
        EXECUTE format('SELECT * from ref_table_%s FOR UPDATE', refshardid::text);
      END
    $$;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-end"
{
    END;
}

step "s2-update-dist-table"
{
    update dist_table set b = 2 where a = 1;
}

step "s2-lock-ref-table-placement-on-coordinator"
{
    DO $$
      DECLARE refshardid int;
      BEGIN
        SELECT shardid INTO refshardid FROM pg_dist_shard WHERE logicalrelid='ref_table'::regclass;
        EXECUTE format('SELECT * from ref_table_%s FOR UPDATE', refshardid::text);
      END
    $$;
}

step "s2-view-dist"
{
	SELECT query, query_hostname, query_hostport, master_query_host_name, master_query_host_port, state, wait_event_type, wait_event, usename, datname FROM citus_dist_stat_activity WHERE query NOT ILIKE '%pg_prepared_xacts%' AND query NOT ILIKE '%COMMIT%' ORDER BY query DESC;	
}

step "s2-view-worker"
{
	SELECT query, query_hostname, query_hostport, master_query_host_name, 
           master_query_host_port, state, wait_event_type, wait_event, usename, datname 
    FROM citus_worker_stat_activity
    WHERE query NOT ILIKE '%pg_prepared_xacts%' AND 
          query NOT ILIKE '%COMMIT%' AND
          query NOT ILIKE '%dump_local_wait_edges%'
    ORDER BY query, query_hostport DESC;
}


step "s2-sleep"
{
	SELECT pg_sleep(0.5);
}

step "s2-active-transactions"
{
	-- Admin should be able to see all transactions
	SELECT count(*) FROM get_all_active_transactions();
	SELECT count(*) FROM get_global_active_transactions();
}

# we disable the daemon during the regression tests in order to get consistent results
# thus we manually issue the deadlock detection 
session "deadlock-checker"

# we issue the checker not only when there are deadlocks to ensure that we never cancel
# backend inappropriately
step "deadlock-checker-call"
{
  SELECT check_distributed_deadlocks();
}

# verify that locks on the placement of the reference table on the coordinator is
# taken into account when looking for distributed deadlocks
permutation "s1-begin" "s2-begin" "s1-update-dist-table" "s2-lock-ref-table-placement-on-coordinator" "s1-lock-ref-table-placement-on-coordinator" "s2-update-dist-table" "deadlock-checker-call" "s1-end" "s2-end"

# verify that *_dist_stat_activity() functions return the correct result when query
# has a task on the coordinator.
permutation "s1-begin" "s2-begin" "s1-update-ref-table" "s2-sleep" "s2-view-dist" "s2-view-worker" "s2-end" "s1-end"

# verify that get_*_active_transactions() functions return the correct result when
# the query has a task on the coordinator.
permutation "s1-begin" "s2-begin" "s1-update-ref-table" "s2-active-transactions" "s1-end" "s2-end"
