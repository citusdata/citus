#include "isolation_mx_common.include.spec"

setup
{
  CREATE OR REPLACE FUNCTION trigger_metadata_sync()
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

  CREATE OR REPLACE FUNCTION wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

  CREATE TABLE deadlock_detection_test (user_id int UNIQUE, some_val int);
  INSERT INTO deadlock_detection_test SELECT i, i FROM generate_series(1,7) i;
  SELECT create_distributed_table('deadlock_detection_test', 'user_id');

  CREATE TABLE t2(a int);
  SELECT create_distributed_table('t2', 'a');
}

teardown
{
  DROP FUNCTION trigger_metadata_sync();
  DROP TABLE deadlock_detection_test;
  DROP TABLE t2;
  SET citus.shard_replication_factor = 1;
  SELECT citus_internal.restore_isolation_tester_func();
}

session "s1"

step "increase-retry-interval"
{
  ALTER SYSTEM SET citus.metadata_sync_retry_interval TO 20000;
}

step "reset-retry-interval"
{
  ALTER SYSTEM RESET citus.metadata_sync_retry_interval;
}

step "enable-deadlock-detection"
{
  ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO 1.1;
}

step "disable-deadlock-detection"
{
  ALTER SYSTEM SET citus.distributed_deadlock_detection_factor TO -1;
}

step "reload-conf"
{
    SELECT pg_reload_conf();
}

step "s1-begin"
{
  BEGIN;
}

step "s1-update-1"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 1;
}

step "s1-update-2"
{
  UPDATE deadlock_detection_test SET some_val = 1 WHERE user_id = 2;
}

step "s1-commit"
{
  COMMIT;
}

step "s1-count-daemons"
{
  SELECT count(*) FROM pg_stat_activity WHERE application_name LIKE 'Citus Met%';
}

step "s1-cancel-metadata-sync"
{
  SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE application_name LIKE 'Citus Met%';
  SELECT pg_sleep(2);
}

session "s2"

step "s2-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

step "s2-stop-connection"
{
	SELECT stop_session_level_connection_to_node();
}

step "s2-begin-on-worker"
{
	SELECT run_commands_on_session_level_connection_to_node('BEGIN');
}

step "s2-update-1-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 1');
}

step "s2-update-2-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('UPDATE deadlock_detection_test SET some_val = 2 WHERE user_id = 2');
}

step "s2-truncate-on-worker"
{
    SELECT run_commands_on_session_level_connection_to_node('TRUNCATE t2');
}

step "s2-commit-on-worker"
{
  SELECT run_commands_on_session_level_connection_to_node('COMMIT');
}

session "s3"

step "s3-invalidate-metadata"
{
    update pg_dist_node SET metadatasynced = false;
}

step "s3-resync"
{
  SELECT trigger_metadata_sync();
  SELECT pg_sleep(2);
}

// Backends can block metadata sync. The following test verifies that if this happens,
// we still do distributed deadlock detection. In the following, s2-truncate-on-worker
// causes the concurrent metadata sync to be blocked. But s2 and s1 themselves are
// themselves involved in a distributed deadlock.
// See https://github.com/citusdata/citus/issues/4393 for more details.
permutation "enable-deadlock-detection" "reload-conf" "s2-start-session-level-connection" "s1-begin" "s1-update-1" "s2-begin-on-worker" "s2-update-2-on-worker" "s2-truncate-on-worker" "s3-invalidate-metadata" "s3-resync" "s2-update-1-on-worker" "s1-update-2" "s1-commit" "s2-commit-on-worker" "disable-deadlock-detection" "reload-conf" "s2-stop-connection"

// Test that when metadata sync is waiting for locks, cancelling it terminates it.
// This is important in cases where the metadata sync daemon itself is involved in a deadlock.
permutation "increase-retry-interval" "reload-conf" "s2-start-session-level-connection" "s2-begin-on-worker" "s2-truncate-on-worker" "s3-invalidate-metadata" "s3-resync" "s1-count-daemons" "s1-cancel-metadata-sync" "s1-count-daemons" "reset-retry-interval" "reload-conf" "s2-commit-on-worker" "s2-stop-connection" "s3-resync"
