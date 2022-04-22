//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create append distributed table to test behavior of COPY in concurrent operations
setup
{
      SELECT citus_internal.replace_isolation_tester_func();
  SELECT citus_internal.refresh_isolation_tester_prepared_statement();
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE hash_copy(id integer, data text, int_data int);
	SELECT create_distributed_table('hash_copy', 'id');
}

// drop distributed table
teardown
{
	DROP TABLE IF EXISTS hash_copy CASCADE;
 SELECT citus_internal.restore_isolation_tester_func();
}

// session 1
session "s1"
step "s1-initialize" { COPY hash_copy FROM PROGRAM 'echo 0, a, 0 && echo 1, b, 1 && echo 2, c, 2 && echo 3, d, 3 && echo 4, e, 4' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-copy" { COPY hash_copy FROM PROGRAM 'echo 5, f, 5 && echo 6, g, 6 && echo 7, h, 7 && echo 8, i, 8 && echo 9, j, 9' WITH CSV; }
step "s1-copy-additional-column" { COPY hash_copy FROM PROGRAM 'echo 5, f, 5, 5 && echo 6, g, 6, 6 && echo 7, h, 7, 7 && echo 8, i, 8, 8 && echo 9, j, 9, 9' WITH CSV; }
step "s1-router-select" { SELECT * FROM hash_copy WHERE id = 1; }
step "s1-real-time-select" { SELECT * FROM hash_copy ORDER BY 1, 2; }
step "s1-adaptive-select"
{
		SET citus.enable_repartition_joins TO ON;
	SELECT * FROM hash_copy AS t1 JOIN hash_copy AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s1-insert" { INSERT INTO hash_copy VALUES(0, 'k', 0); }
step "s1-insert-select" { INSERT INTO hash_copy SELECT * FROM hash_copy; }
step "s1-update" { UPDATE hash_copy SET data = 'l' WHERE id = 0; }
step "s1-delete" { DELETE FROM hash_copy WHERE id = 1; }
step "s1-truncate" { TRUNCATE hash_copy; }
step "s1-drop" { DROP TABLE hash_copy; }
step "s1-ddl-create-index" { CREATE INDEX hash_copy_index ON hash_copy(id); }
step "s1-ddl-drop-index" { DROP INDEX hash_copy_index; }
step "s1-ddl-add-column" { ALTER TABLE hash_copy ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE hash_copy DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE hash_copy RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('hash_copy'); }
step "s1-master-modify-multiple-shards" { DELETE FROM hash_copy; }
step "s1-master-drop-all-shards" { SELECT citus_drop_all_shards('hash_copy'::regclass, 'public', 'hash_copy'); }
step "s1-create-non-distributed-table" { CREATE TABLE hash_copy(id integer, data text, int_data int); COPY hash_copy FROM PROGRAM 'echo 0, a, 0 && echo 1, b, 1 && echo 2, c, 2 && echo 3, d, 3 && echo 4, e, 4' WITH CSV; }
step "s1-distribute-table" { SELECT create_distributed_table('hash_copy', 'id'); }
step "s1-select-count" { SELECT COUNT(*) FROM hash_copy; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''hash_copy\_%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''hash_copy%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }
step "s1-recreate-with-replication-2"
{
	DROP TABLE hash_copy;
	SET citus.shard_replication_factor TO 2;
	CREATE TABLE hash_copy(id integer, data text, int_data int);
	SELECT create_distributed_table('hash_copy', 'id');
}

// session 2
session "s2"
step "s2-copy" { COPY hash_copy FROM PROGRAM 'echo 5, f, 5 && echo 6, g, 6 && echo 7, h, 7 && echo 8, i, 8 && echo 9, j, 9' WITH CSV; }
step "s2-router-select" { SELECT * FROM hash_copy WHERE id = 1; }
step "s2-real-time-select" { SELECT * FROM hash_copy ORDER BY 1, 2; }
step "s2-adaptive-select"
{
		SET citus.enable_repartition_joins TO ON;
	SELECT * FROM hash_copy AS t1 JOIN hash_copy AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s2-insert" { INSERT INTO hash_copy VALUES(0, 'k', 0); }
step "s2-insert-select" { INSERT INTO hash_copy SELECT * FROM hash_copy; }
step "s2-update" { UPDATE hash_copy SET data = 'l' WHERE id = 0; }
step "s2-delete" { DELETE FROM hash_copy WHERE id = 1; }
step "s2-truncate" { TRUNCATE hash_copy; }
step "s2-drop" { DROP TABLE hash_copy; }
step "s2-ddl-create-index" { CREATE INDEX hash_copy_index ON hash_copy(id); }
step "s2-ddl-drop-index" { DROP INDEX hash_copy_index; }
step "s2-flaky-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY flaky_hash_copy_index ON hash_copy(id); }
step "s2-ddl-add-column" { ALTER TABLE hash_copy ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE hash_copy DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE hash_copy RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('hash_copy'); }
step "s2-master-modify-multiple-shards" { DELETE FROM hash_copy; }
step "s2-master-drop-all-shards" { SELECT citus_drop_all_shards('hash_copy'::regclass, 'public', 'hash_copy'); }
step "s2-distribute-table" { SELECT create_distributed_table('hash_copy', 'id'); }

// permutations - COPY vs COPY
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-copy" "s1-commit" "s1-select-count"

// permutations - COPY first
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-adaptive-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-copy" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-flaky-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-copy-additional-column" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-master-drop-all-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-copy" "s2-distribute-table" "s1-commit" "s1-select-count"

// permutations - COPY first (replication factor 2)
permutation "s1-recreate-with-replication-2" "s1-initialize" "s1-begin" "s1-copy" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-recreate-with-replication-2" "s1-initialize" "s1-begin" "s1-copy" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-recreate-with-replication-2" "s1-initialize" "s1-begin" "s1-copy" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-recreate-with-replication-2" "s1-initialize" "s1-begin" "s1-copy" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"

// permutations - COPY second
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-adaptive-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-copy" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-copy" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-copy" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-copy" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-copy" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-drop-all-shards" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-distribute-table" "s2-copy" "s1-commit" "s1-select-count"
