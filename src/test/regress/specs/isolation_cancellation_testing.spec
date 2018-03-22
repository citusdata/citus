# see that we can cause a session to block at a point of our choosing

session "s1"

setup { BEGIN; SET application_name TO 'gdb-process' }
step "commit" { COMMIT; }

step "create-table"
{
  CREATE TABLE test (a int, b int);
  SELECT create_distributed_table('test', 'a');
}

session "gdb"

step "gdb-attach"
{
  WITH pid AS (
    SELECT pid FROM pg_stat_activity WHERE application_name = 'gdb-process'
  ) SELECT citus.gdb_attach(pid.pid) FROM pid;

  -- ShardStorageType is called just after HOLD_INTERRUPTS()
  SELECT citus.run_command('!nested-cancel CreateShardsWithRoundRobinPolicy ShardStorageType');
  SELECT citus.run_command('continue');
}

# try to cancel during create_distributed_table
permutation "gdb-attach" "create-table" "commit"
