// This file tests that logical replication works even when the table that's
// being moved contains columns that don't allow for binary encoding
setup
{
	SET citus.shard_count TO 1;
	SET citus.shard_replication_factor TO 1;
	ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 45076800;
	CREATE TABLE t_nonbinary(id bigserial, nonbinary aclitem);
	SELECT create_distributed_table('t_nonbinary', 'id');
	INSERT INTO t_nonbinary (SELECT i, 'user postgres=r/postgres' FROM generate_series(1, 5) i);
}

teardown
{
	DROP TABLE t_nonbinary;
}


session "s1"

step "s1-move-placement"
{
	SELECT citus_move_shard_placement(45076800, 'localhost', 57637, 'localhost', 57638, shard_transfer_mode:='force_logical');
}

step "s1-select"
{
  SELECT * FROM t_nonbinary order by id;
}

session "s2"
step "s2-insert"
{
	INSERT INTO t_nonbinary (SELECT i, 'user postgres=r/postgres' FROM generate_series(6, 10) i);
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

permutation "s3-acquire-advisory-lock" "s1-move-placement" "s2-insert" "s3-release-advisory-lock" "s1-select"
