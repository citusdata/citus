setup
{
	SET citus.shard_replication_factor TO 1;
    SET citus.shard_count TO 4;
	CREATE TABLE source_table(a int, b int);
	SELECT create_distributed_table('source_table', 'a');
    SET citus.shard_count TO 3;
	CREATE TABLE target_table(a int, b int);
	SELECT create_distributed_table('target_table', 'a');

    INSERT INTO source_table SELECT i, i * i FROM generate_series(1, 10) i;
}

teardown
{
	DROP TABLE IF EXISTS source_table;
	DROP TABLE IF EXISTS target_table;
}

session "s1"
step "s1-begin" { BEGIN; }
step "s1-end" { END; }
step "s1-repartitioned-insert-select" { INSERT INTO target_table SELECT * FROM source_table; }
step "s1-select-target" { SELECT * FROM target_table ORDER BY a; }

session "s2"
step "s2-begin" { BEGIN; }
step "s2-end" { END; }
step "s2-delete-from-source" { DELETE FROM source_table; }
step "s2-update-source" { UPDATE source_table SET b = 50 - b; }
step "s2-insert-into-source" { INSERT INTO source_table VALUES (0, 0); }
step "s2-delete-from-target" { DELETE FROM target_table; }
step "s2-update-target" { UPDATE target_table SET b = 50 - b; }
step "s2-insert-into-target" { INSERT INTO target_table VALUES (0, 0); }

// INSERT/INTO shouldn't block DML on source_table
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-delete-from-source" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-update-source" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-insert-into-source" "s1-end" "s2-end" "s1-select-target"

// INSERT/INTO shouldn't be blocked by DML on source_table
permutation "s1-begin" "s2-begin" "s2-delete-from-source" "s1-repartitioned-insert-select" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s2-update-source" "s1-repartitioned-insert-select" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s2-insert-into-source" "s1-repartitioned-insert-select" "s1-end" "s2-end" "s1-select-target"

// INSERT/INTO should block UPDATE/DELETE on target_table, but not INSERT
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-delete-from-target" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-update-target" "s1-end" "s2-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s1-repartitioned-insert-select" "s2-insert-into-target" "s1-end" "s2-end" "s1-select-target"

// INSERT/INTO should be blocked by UPDATE/DELETe on target_table, but not INSERT
permutation "s1-begin" "s2-begin" "s2-delete-from-target" "s1-repartitioned-insert-select" "s2-end" "s1-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s2-update-target" "s1-repartitioned-insert-select" "s2-end" "s1-end" "s1-select-target"
permutation "s1-begin" "s2-begin" "s2-insert-into-target" "s1-repartitioned-insert-select" "s2-end" "s1-end" "s1-select-target"
