#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create append distributed table to test behavior of COPY in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE range_copy(id integer, data text, int_data int);
	SELECT create_distributed_table('range_copy', 'id', 'append');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS range_copy CASCADE;
}

# session 1
session "s1"
step "s1-initialize" { COPY range_copy FROM PROGRAM 'echo 0, a, 0 && echo 1, b, 1 && echo 2, c, 2 && echo 3, d, 3 && echo 4, e, 4' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-copy" { COPY range_copy FROM PROGRAM 'echo 5, f, 5 && echo 6, g, 6 && echo 7, h, 7 && echo 8, i, 8 && echo 9, j, 9' WITH CSV; }
step "s1-copy-additional-column" { COPY range_copy FROM PROGRAM 'echo 5, f, 5, 5 && echo 6, g, 6, 6 && echo 7, h, 7, 7 && echo 8, i, 8, 8 && echo 9, j, 9, 9' WITH CSV; }
step "s1-router-select" { SELECT * FROM range_copy WHERE id = 1; }
step "s1-real-time-select" { SELECT * FROM range_copy ORDER BY 1, 2; }
step "s1-task-tracker-select"
{
	SET citus.task_executor_type TO "task-tracker";
	SELECT * FROM range_copy AS t1 JOIN range_copy AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s1-insert" { INSERT INTO range_copy VALUES(0, 'k', 0); }
step "s1-insert-select" { INSERT INTO range_copy SELECT * FROM range_copy; }
step "s1-update" { UPDATE range_copy SET data = 'l' WHERE id = 0; }
step "s1-delete" { DELETE FROM range_copy WHERE id = 1; }
step "s1-truncate" { TRUNCATE range_copy; }
step "s1-drop" { DROP TABLE range_copy; }
step "s1-ddl-create-index" { CREATE INDEX range_copy_index ON range_copy(id); }
step "s1-ddl-drop-index" { DROP INDEX range_copy_index; }
step "s1-ddl-add-column" { ALTER TABLE range_copy ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE range_copy DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE range_copy RENAME data TO new_column; }
step "s1-ddl-unique-constraint" { ALTER TABLE range_copy ADD CONSTRAINT range_copy_unique UNIQUE(id); }
step "s1-table-size" { SELECT citus_total_relation_size('range_copy'); }
step "s1-master-modify-multiple-shards" { SELECT master_modify_multiple_shards('DELETE FROM range_copy;'); }
step "s1-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM range_copy WHERE id <= 4;'); }
step "s1-master-drop-all-shards" { SELECT master_drop_all_shards('range_copy'::regclass, 'public', 'range_copy'); }
step "s1-create-non-distributed-table" { CREATE TABLE range_copy(id integer, data text, int_data int); }
step "s1-distribute-table" { SELECT create_distributed_table('range_copy', 'id', 'range'); }
step "s1-select-count" { SELECT COUNT(*) FROM range_copy; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''range_copy%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''range_copy%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }

step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-copy" { COPY range_copy FROM PROGRAM 'echo 5, f, 5 && echo 6, g, 6 && echo 7, h, 7 && echo 8, i, 8 && echo 9, j, 9' WITH CSV; }
step "s2-copy-additional-column" { COPY range_copy FROM PROGRAM 'echo 5, f, 5, 5 && echo 6, g, 6, 6 && echo 7, h, 7, 7 && echo 8, i, 8, 8 && echo 9, j, 9, 9' WITH CSV; }
step "s2-router-select" { SELECT * FROM range_copy WHERE id = 1; }
step "s2-real-time-select" { SELECT * FROM range_copy ORDER BY 1, 2; }
step "s2-task-tracker-select"
{
	SET citus.task_executor_type TO "task-tracker";
	SELECT * FROM range_copy AS t1 JOIN range_copy AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s2-insert" { INSERT INTO range_copy VALUES(0, 'k', 0); }
step "s2-insert-select" { INSERT INTO range_copy SELECT * FROM range_copy; }
step "s2-update" { UPDATE range_copy SET data = 'l' WHERE id = 0; }
step "s2-delete" { DELETE FROM range_copy WHERE id = 1; }
step "s2-truncate" { TRUNCATE range_copy; }
step "s2-drop" { DROP TABLE range_copy; }
step "s2-ddl-create-index" { CREATE INDEX range_copy_index ON range_copy(id); }
step "s2-ddl-drop-index" { DROP INDEX range_copy_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY range_copy_index ON range_copy(id); }
step "s2-ddl-add-column" { ALTER TABLE range_copy ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE range_copy DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE range_copy RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('range_copy'); }
step "s2-master-modify-multiple-shards" { SELECT master_modify_multiple_shards('DELETE FROM range_copy;'); }
step "s2-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM range_copy WHERE id <= 4;'); }
step "s2-master-drop-all-shards" { SELECT master_drop_all_shards('range_copy'::regclass, 'public', 'range_copy'); }
step "s2-distribute-table" { SELECT create_distributed_table('range_copy', 'id', 'range'); }

# permutations - COPY vs COPY
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-copy" "s1-commit" "s1-select-count"

# permutations - COPY first
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-copy" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-copy-additional-column" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-master-apply-delete-command" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-copy" "s2-master-drop-all-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-copy" "s2-distribute-table" "s1-commit" "s1-select-count"

# permutations - COPY second
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-copy" "s1-commit" "s1-select-count"
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
permutation "s1-initialize" "s1-begin" "s1-master-apply-delete-command" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-drop-all-shards" "s2-copy" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-distribute-table" "s2-copy" "s1-commit" "s1-select-count"
