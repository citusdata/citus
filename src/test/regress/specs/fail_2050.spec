# see what happens when we fail a worker in the middle of a real-time query

setup
{
  SET citus.shard_replication_factor TO 1;
  
  CREATE TABLE tt1(id int, value_1 varchar(20));
  INSERT INTO tt1 select x,x::text from generate_series(1, 100000) as f(x);
  SELECT create_distributed_table('tt1', 'id');

  CREATE TABLE tt2(id int, value_1 varchar(20));
  SELECT create_distributed_table('tt2', 'value_1');
}

teardown
{
  DROP TABLE tt1, tt2;
}

session "s1"

setup
{
  SET application_name TO 'gdb-process';
}

step "insert-select"
{
  BEGIN;
  INSERT INTO tt2 SELECT * FROM tt1;
}

step "select"
{
  SELECT 1;
}

step "rollback"
{
  ROLLBACK
}

teardown { SELECT pg_advisory_unlock_all(); }

session "gdb"

setup { SELECT pg_advisory_lock(1); }

step "attach"
{
  WITH pid AS (
    SELECT pid FROM pg_stat_activity WHERE application_name = 'gdb-process' LIMIT 1
  ) SELECT citus.gdb_attach(pid.pid) FROM pid;
  SELECT citus.run_command('continue');
}

step "break-before-send"
{
  -- we're already attached
  SELECT citus.run_command('!interrupt');

  -- fail one of the workers right before sending the query to it
  SELECT citus.run_command('!break ExecuteQueryIntoDestReceiver');
  SELECT citus.run_command('continue');
}

step "break-on-receive"
{
  -- we're already attached
  SELECT citus.run_command('!interrupt');

  -- fail one of the workers once we start getting results
  SELECT citus.run_command('!break CitusCopyDestReceiverReceive');
  SELECT citus.run_command('continue');
}

step "partition-worker"
{
  -- kill those COPYs
  SELECT master_run_on_worker(ARRAY['localhost'], ARRAY[57637], ARRAY['
   WITH pids AS (
	   SELECT pg_terminate_backend(pid)
	   FROM pg_stat_activity
           WHERE query LIKE ''COPY%''
  ) SELECT count(1) FROM pids;
  '], false);

  SELECT citus.run_command('!worker-partition add 57637');
}

step "reconnect-worker"
{
  SELECT citus.run_command('!worker-partition remove 57637');
}

step "continue"
{
  SELECT pg_advisory_unlock_all();
}

permutation "attach"
# if we partition before opening connections then it fails as expected
permutation "break-before-send" "insert-select" "partition-worker" "continue" "select" "reconnect-worker" "rollback"
# if we partition once we start getting data then we crash
permutation "break-on-receive" "insert-select" "partition-worker" "continue" "select" "reconnect-worker" "rollback"
