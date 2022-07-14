setup
{
    SET citus.shard_count TO 2;
	SET citus.shard_replication_factor TO 1;

    CREATE TABLE ref_table_1(id int PRIMARY KEY, value int);
	SELECT create_reference_table('ref_table_1');

    CREATE TABLE ref_table_2(id int PRIMARY KEY REFERENCES ref_table_1(id) ON DELETE CASCADE ON UPDATE CASCADE);
	SELECT create_reference_table('ref_table_2');

    CREATE TABLE dist_table(id int PRIMARY KEY, value int REFERENCES ref_table_2(id) ON DELETE CASCADE ON UPDATE CASCADE);
    SELECT create_distributed_table('dist_table', 'id');

    INSERT INTO ref_table_1 VALUES (1, 1), (3, 3), (5, 5), (7, 7), (9, 9), (11, 11);
    INSERT INTO ref_table_2 VALUES (1), (3), (7), (9), (11);
    INSERT INTO dist_table VALUES (1, 1), (3, 3), (7, 7), (9, 9), (11, 11);

    SELECT get_shard_id_for_distribution_column('dist_table', 5) INTO selected_shard_for_dist_table;
}

teardown
{
	DROP TABLE ref_table_1, ref_table_2, dist_table, selected_shard_for_dist_table;
}

session "s1"

step "s1-begin"
{
	BEGIN;
}

step "s1-delete-table-1"
{
    DELETE FROM ref_table_1 WHERE value = 5;
}

step "s1-update-table-1"
{
    UPDATE ref_table_1 SET id = id + 1 WHERE value = 5;
}

step "s1-insert-table-1"
{
    INSERT INTO ref_table_2 VALUES (5);
    INSERT INTO dist_table VALUES (5, 5);
}

step "s1-select-table-1"
{
    SELECT * FROM ref_table_1 ORDER BY id, value;
}

step "s1-select-dist-table"
{
    SELECT * FROM dist_table ORDER BY id, value;
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

step "s2-move-shards"
{
    SELECT master_move_shard_placement((SELECT * FROM selected_shard_for_dist_table), 'localhost', 57637, 'localhost', 57638, 'force_logical');
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

permutation "s1-insert-table-1" "s3-acquire-advisory-lock" "s2-begin" "s2-move-shards" "s1-delete-table-1" "s3-release-advisory-lock" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
permutation "s1-insert-table-1" "s3-acquire-advisory-lock" "s2-begin" "s2-move-shards" "s1-update-table-1" "s3-release-advisory-lock" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
permutation "s3-acquire-advisory-lock" "s2-begin" "s2-move-shards" "s1-insert-table-1" "s3-release-advisory-lock" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
permutation "s1-insert-table-1" "s3-acquire-advisory-lock" "s2-begin" "s2-move-shards" "s1-select-table-1" "s3-release-advisory-lock" "s2-commit"

permutation "s1-insert-table-1" "s2-begin" "s1-begin" "s1-delete-table-1" "s2-move-shards" "s1-commit" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
permutation "s1-insert-table-1" "s2-begin" "s1-begin" "s1-update-table-1" "s2-move-shards" "s1-commit" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
permutation "s2-begin" "s1-begin" "s1-insert-table-1" "s2-move-shards" "s1-commit" "s2-commit" "s1-select-table-1" "s1-select-dist-table"
