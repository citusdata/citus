# see what happens when we fail a worker in the middle of a real-time query

setup
{
  CREATE TABLE test_table (a int, b int);
  SELECT create_distributed_table('test_table', 'a');
  INSERT INTO test_table VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5);
}

teardown
{
  DROP TABLE test_table;
}

session "s1"

setup
{
  BEGIN;
  SET application_name TO 'gdb-process';
  SELECT assign_distributed_transaction_id(1000, 1000, now());
}

step "select" { SELECT * FROM test_table; }
step "commit" { COMMIT; }

session "gdb"

setup { SELECT pg_advisory_lock(1); }

step "gdb-break"
{
  WITH pid AS (
    SELECT pid FROM pg_stat_activity WHERE application_name = 'gdb-process'
  ) SELECT citus.gdb_attach(pid.pid) FROM pid;

  SELECT citus.run_command('!break BeginOrContinueCoordinatedTransaction');
  SELECT citus.run_command('continue');
}

step "fail-worker-backends"
{
  SELECT master_run_on_worker(ARRAY['localhost'], ARRAY[57637], ARRAY['
   WITH pids AS (
	   SELECT process_id, pg_terminate_backend(process_id)
	   FROM get_all_active_transactions()
	   WHERE transaction_number = 1000
  ) SELECT process_id FROM pids;
  '], false)
}

step "continue"
{
  SELECT pg_advisory_unlock_all();
}

permutation "gdb-break" "select" "fail-worker-backends" "continue" "commit"
