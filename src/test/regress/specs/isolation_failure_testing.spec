# see that we can cause a session to block at a point of our choosing

session "s1"

setup { BEGIN; SET deadlock_timeout TO '100ms'; }
step "s1-lock-1" { SELECT pg_advisory_xact_lock(1); }
step "s1-lock-2" { SELECT pg_advisory_xact_lock(2); }
step "s1-commit" { COMMIT; }

session "s2"

setup { BEGIN; SET deadlock_timeout TO '100ms'; SET application_name TO 'gdb-process' }
step "s2-lock-1" { SELECT pg_advisory_xact_lock(1); }
step "s2-lock-2" { SELECT pg_advisory_xact_lock(2); }
step "s2-commit" { COMMIT; }

step "s2-lock-1-gdb"
{
  SELECT 1;
}

session "gdb"

step "gdb-break"
{
  WITH pid AS (
    SELECT pid FROM pg_stat_activity WHERE application_name = 'gdb-process'
  ) SELECT citus.gdb_attach(pid.pid) FROM pid;

  SELECT pg_sleep(1);
  SELECT citus.run_command('!break NeedsDistributedPlanning');
  SELECT pg_sleep(1);
  SELECT citus.run_command('continue');
}

# a regular deadlock is detected
permutation "s2-lock-2" "s1-lock-1" "s1-lock-2" "s2-lock-1" "s2-commit" "s1-commit"
# now try to inject a deadlock with gdb
permutation "s2-lock-2" "s1-lock-1" "s1-lock-2" "gdb-break" "s2-lock-1-gdb" "s2-commit" "s1-commit"
