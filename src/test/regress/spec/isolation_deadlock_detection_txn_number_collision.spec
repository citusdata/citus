// Transaction numbers are only unique per initiator node. This test makes a
// transaction that looks like it was initiated on another node share its
// transaction number with an unrelated local transaction, and makes the former
// the youngest participant of a distributed deadlock. The deadlock detector
// must cancel the local participant of the deadlock, and must not cancel the
// unrelated local transaction that happens to have the same number.

setup
{
  -- with a single placement the deadlock spans both workers, so only the
  -- distributed deadlock detector can resolve it
  SET citus.shard_replication_factor TO 1;
  SET citus.next_shard_id TO 8790000;
  CREATE TABLE txn_number_collision (user_id int PRIMARY KEY, some_val int);
  INSERT INTO txn_number_collision SELECT i, i FROM generate_series(1,7) i;
  SELECT create_distributed_table('txn_number_collision', 'user_id');
}

teardown
{
  DROP TABLE txn_number_collision;
}

// unrelated local transaction, blocked behind s4 so that a wrong cancellation
// would be visible; its transaction number is assigned before it blocks
session "s1"

step "s1-begin"
{
  BEGIN;
}

step "s1-update-3"
{
  UPDATE txn_number_collision SET some_val = 1001 WHERE user_id = 3;
}

step "s1-commit"
{
  COMMIT;
}

// pretends to be initiated by another node, with the transaction number of s1
session "s2"

step "s2-begin-with-s1-txn-number"
{
  BEGIN;
  SELECT assign_distributed_transaction_id(9999, transaction_number, now())
  FROM get_all_active_transactions() t JOIN pg_stat_activity a ON (t.process_id = a.pid)
  WHERE a.pid <> pg_backend_pid() AND a.query LIKE '%some_val = 1001%';
}

step "s2-update-1"
{
  UPDATE txn_number_collision SET some_val = 2 WHERE user_id = 1;
}

step "s2-update-2"
{
  UPDATE txn_number_collision SET some_val = 2 WHERE user_id = 2;
}

step "s2-commit"
{
  COMMIT;
}

// local participant of the deadlock, older than s2
session "s3"

step "s3-begin"
{
  BEGIN;
}

step "s3-update-1"
{
  UPDATE txn_number_collision SET some_val = 3 WHERE user_id = 1;
}

step "s3-update-2"
{
  UPDATE txn_number_collision SET some_val = 3 WHERE user_id = 2;
}

step "s3-commit"
{
  COMMIT;
}

session "s4"

step "s4-begin"
{
  BEGIN;
}

step "s4-update-3"
{
  UPDATE txn_number_collision SET some_val = 4 WHERE user_id = 3;
}

step "s4-commit"
{
  COMMIT;
}

session "deadlock-checker"

step "deadlock-checker-call"
{
  SELECT check_distributed_deadlocks();
}

permutation "s4-begin" "s4-update-3" "s1-begin" "s1-update-3" "s3-begin" "s3-update-1" "s2-begin-with-s1-txn-number" "s2-update-2" "s2-update-1" "s3-update-2" "deadlock-checker-call" "s3-commit" "s2-commit" "s4-commit" "s1-commit"
