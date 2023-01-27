#include "isolation_mx_common.include.spec"

setup
{
  SET citus.shard_count to 1;
  SET citus.shard_replication_factor to 1;

  CREATE TABLE t1 (id int PRIMARY KEY, value int);
  SELECT create_distributed_table('t1', 'id');

  CREATE TABLE t2 (id int PRIMARY KEY, value int);
  SELECT create_distributed_table('t2', 'id');

  CREATE TABLE r (id int PRIMARY KEY, value int);
  SELECT create_reference_table('r');
  SELECT get_shard_id_for_distribution_column('t1', 5) INTO selected_shard_for_test_table;
}

setup {
  ALTER TABLE t1 ADD CONSTRAINT t1_t2_fkey FOREIGN KEY (id) REFERENCES t2(id);
}

setup {
  ALTER TABLE t1 ADD CONSTRAINT t1_r_fkey FOREIGN KEY (value) REFERENCES r(id);
}

teardown
{
  DROP TABLE t1;
  DROP TABLE t2;
  DROP TABLE r;
  DROP TABLE selected_shard_for_test_table;
}

session "s1"

step "s1-start-session-level-connection"
{
	SELECT start_session_level_connection_to_node('localhost', 57638);
}

// This inserts a foreign key violation directly into the shard on the target
// worker. Since we're not validating the foreign key on the new shard on
// purpose we expect no errors.
step "s1-insert-violation-into-shard"
{
	SELECT run_commands_on_session_level_connection_to_node(format('INSERT INTO t1_%s VALUES (-1, -1)', (SELECT * FROM selected_shard_for_test_table)));
}

session "s2"

step "s2-move-placement"
{
	SELECT master_move_shard_placement((SELECT * FROM selected_shard_for_test_table), 'localhost', 57637, 'localhost', 57638);
}

session "s3"

// this advisory lock with (almost) random values are only used
// for testing purposes. For details, check Citus' logical replication
// source code
step "s3-acquire-advisory-lock"
{
	SELECT pg_advisory_lock(44000, 55152);
}

step "s3-release-advisory-lock"
{
	SELECT pg_advisory_unlock(44000, 55152);
}

permutation "s1-start-session-level-connection" "s3-acquire-advisory-lock" "s2-move-placement" "s1-start-session-level-connection" "s1-insert-violation-into-shard" "s3-release-advisory-lock"
