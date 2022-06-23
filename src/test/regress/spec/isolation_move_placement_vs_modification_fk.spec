setup
{
  SET citus.shard_count to 2;
  SET citus.shard_replication_factor to 1;
	SELECT setval('pg_dist_shardid_seq',
		CASE WHEN nextval('pg_dist_shardid_seq') > 1699999 OR nextval('pg_dist_shardid_seq') < 1600000
			THEN 1600000
			ELSE nextval('pg_dist_shardid_seq')-2
		END);

  CREATE TABLE referenced_table (id int PRIMARY KEY, value int);
  SELECT create_reference_table('referenced_table');

  CREATE TABLE referencing_table (id int PRIMARY KEY, value int);
  SELECT create_distributed_table('referencing_table', 'id');

  SELECT get_shard_id_for_distribution_column('referencing_table', 2) INTO selected_shard_for_test_table;
}

teardown
{
  DROP TABLE referencing_table;
  DROP TABLE referenced_table;
  DROP TABLE selected_shard_for_test_table;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-insert-referenced"
{
  INSERT INTO referenced_table SELECT x,x FROM generate_series(1,10) as f(x);
}

step "s1-insert-referencing"
{
  INSERT INTO referencing_table SELECT x,x FROM generate_series(1,10) as f(x);
}

step "s1-delete"
{
  DELETE FROM referenced_table WHERE id < 5;
}

step "s1-update"
{
  UPDATE referenced_table SET value = 5 WHERE id = 5;
}

step "s1-ddl"
{
  CREATE INDEX referenced_table_index ON referenced_table(id);
}

step "s1-commit"
{
  COMMIT;
}

session "s2"

step "s2-begin"
{
  BEGIN;
}

step "s2-add-fkey"
{
  ALTER TABLE referencing_table ADD CONSTRAINT fkey_const FOREIGN KEY (value) REFERENCES referenced_table(id) ON DELETE CASCADE;
}

step "s2-move-placement-blocking"
{
  SELECT master_move_shard_placement((SELECT * FROM selected_shard_for_test_table), 'localhost', 57638, 'localhost', 57637, shard_transfer_mode:='block_writes');
}

step "s2-move-placement-nonblocking"
{
  SELECT master_move_shard_placement((SELECT * FROM selected_shard_for_test_table), 'localhost', 57638, 'localhost', 57637);
}

step "s2-print-cluster"
{
  -- row count per shard
  SELECT
    nodeport, shardid, success, result
  FROM
    run_command_on_placements('referencing_table', 'select count(*) from %s')
  ORDER BY
    nodeport, shardid;

  -- rows
  SELECT * FROM referencing_table ORDER BY 1;
}

step "s2-commit"
{
  COMMIT;
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

// run master_move_shard_placement while concurrently performing an DML and index creation
// we expect shard_rebalancer on referencing table to get blocked by the DML operation
// on the referenced table.
permutation "s2-add-fkey" "s1-insert-referenced" "s1-insert-referencing" "s1-begin" "s2-begin" "s2-move-placement-blocking" "s1-delete" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-insert-referenced" "s1-insert-referencing" "s1-begin" "s2-begin" "s2-move-placement-blocking" "s1-update" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-insert-referenced" "s1-insert-referencing" "s1-begin" "s2-begin" "s2-move-placement-blocking" "s1-ddl" "s2-commit" "s1-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s1-insert-referenced" "s1-begin" "s2-begin" "s2-move-placement-blocking" "s1-insert-referencing" "s2-commit" "s1-commit" "s2-print-cluster"

permutation "s2-add-fkey" "s3-acquire-advisory-lock" "s1-insert-referenced" "s1-insert-referencing" "s2-begin" "s2-move-placement-nonblocking" "s1-delete" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s3-acquire-advisory-lock" "s1-insert-referenced" "s1-insert-referencing" "s2-begin" "s2-move-placement-nonblocking" "s1-update" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s3-acquire-advisory-lock" "s1-insert-referenced" "s1-insert-referencing" "s2-begin" "s2-move-placement-nonblocking" "s1-ddl" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"
permutation "s2-add-fkey" "s3-acquire-advisory-lock" "s1-insert-referenced" "s2-begin" "s2-move-placement-nonblocking" "s1-insert-referencing" "s3-release-advisory-lock" "s2-commit" "s2-print-cluster"

