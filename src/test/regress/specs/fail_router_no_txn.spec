# see what happens when we fail a worker in the middle of a real-time query

setup
{
  SET citus.shard_replication_factor TO 1;
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
  SET application_name TO 'gdb-process';
}

step "update"
{
  UPDATE test_table SET b = 2 WHERE a = 1; 
}

step "select"
{
  SELECT * FROM test_table WHERE a = 1;
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
  SELECT citus.run_command('!break SendQueryInSingleRowMode');
  SELECT citus.run_command('continue');
}

step "partition-worker"
{
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
permutation "break-before-send" "update" "partition-worker" "continue" "select" "reconnect-worker"
