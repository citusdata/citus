# check that replace_isolation_tester_func correctly replaces the functions isolation
# tester uses while searching for locks. If those functions aren't correctly replaced
# this test will timeout, since isolation tester will never notice that s2 is blocked
# by s1 on a lock it's taken out on one of the workers

setup
{
  SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();

  CREATE TABLE test_locking (a int unique);
  SELECT create_distributed_table('test_locking', 'a');
}

teardown
{
  DROP TABLE test_locking;
  SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "s1-insert-1"
{
  BEGIN;
  INSERT INTO test_locking (a) VALUES (1);
}

step "s1-commit"
{
  COMMIT;
}

session "s2"

step "s2-insert"
{
  BEGIN;
  INSERT INTO test_locking (a) VALUES (1);
}

step "s2-commit"
{
  COMMIT;
}

permutation "s1-insert-1" "s2-insert" "s1-commit" "s2-commit"
