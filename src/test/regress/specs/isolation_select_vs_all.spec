#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create range distributed table to test behavior of SELECT in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE select_append(id integer, data text, int_data int);
	SELECT create_distributed_table('select_append', 'id', 'append');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS select_append CASCADE;
}

# session 1
session "s1"
step "s1-initialize" { COPY select_append FROM PROGRAM 'echo 0, a, 0 && echo 1, b, 1 && echo 2, c, 2 && echo 3, d, 3 && echo 4, e, 4' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-router-select" { SELECT * FROM select_append WHERE id = 1; }
step "s1-real-time-select" { SELECT * FROM select_append ORDER BY 1, 2; }
step "s1-task-tracker-select"
{
	SET citus.task_executor_type TO "task-tracker";
	SELECT * FROM select_append AS t1 JOIN select_append AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s1-insert" { INSERT INTO select_append VALUES(0, 'k', 0); }
step "s1-insert-select" { INSERT INTO select_append SELECT * FROM select_append; }
step "s1-update" { UPDATE select_append SET data = 'l' WHERE id = 0; }
step "s1-delete" { DELETE FROM select_append WHERE id = 1; }
step "s1-truncate" { TRUNCATE select_append; }
step "s1-drop" { DROP TABLE select_append; }
step "s1-ddl-create-index" { CREATE INDEX select_append_index ON select_append(id); }
step "s1-ddl-drop-index" { DROP INDEX select_append_index; }
step "s1-ddl-add-column" { ALTER TABLE select_append ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE select_append DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE select_append RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('select_append'); }
step "s1-master-modify-multiple-shards" { SELECT master_modify_multiple_shards('DELETE FROM select_append;'); }
step "s1-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM select_append WHERE id <= 4;'); }
step "s1-master-drop-all-shards" { SELECT master_drop_all_shards('select_append'::regclass, 'public', 'append_copy'); }
step "s1-create-non-distributed-table" { CREATE TABLE select_append(id integer, data text, int_data int); }
step "s1-distribute-table" { SELECT create_distributed_table('select_append', 'id', 'append'); }
step "s1-select-count" { SELECT COUNT(*) FROM select_append; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''select_append%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''select_append%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-router-select" { SELECT * FROM select_append WHERE id = 1; }
step "s2-real-time-select" { SELECT * FROM select_append ORDER BY 1, 2; }
step "s2-task-tracker-select"
{
	SET citus.task_executor_type TO "task-tracker";
	SELECT * FROM select_append AS t1 JOIN select_append AS t2 ON t1.id = t2.int_data ORDER BY 1, 2, 3, 4;
}
step "s2-insert" { INSERT INTO select_append VALUES(0, 'k', 0); }
step "s2-insert-select" { INSERT INTO select_append SELECT * FROM select_append; }
step "s2-update" { UPDATE select_append SET data = 'l' WHERE id = 0; }
step "s2-delete" { DELETE FROM select_append WHERE id = 1; }
step "s2-truncate" { TRUNCATE select_append; }
step "s2-drop" { DROP TABLE select_append; }
step "s2-ddl-create-index" { CREATE INDEX select_append_index ON select_append(id); }
step "s2-ddl-drop-index" { DROP INDEX select_append_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY select_append_index ON select_append(id); }
step "s2-ddl-add-column" { ALTER TABLE select_append ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE select_append DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE select_append RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('select_append'); }
step "s2-master-modify-multiple-shards" { SELECT master_modify_multiple_shards('DELETE FROM select_append;'); }
step "s2-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM select_append WHERE id <= 4;'); }
step "s2-master-drop-all-shards" { SELECT master_drop_all_shards('select_append'::regclass, 'public', 'append_copy'); }
step "s2-distribute-table" { SELECT create_distributed_table('select_append', 'id', 'append'); }

# permutations - SELECT vs SELECT
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-task-tracker-select" "s1-commit" "s1-select-count"

# permutations - router SELECT first
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-router-select" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-router-select" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-router-select" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-master-apply-delete-command" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-master-drop-all-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-router-select" "s2-distribute-table" "s1-commit" "s1-select-count"

# permutations - router SELECT second
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-router-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-router-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-router-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-router-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-router-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-router-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-apply-delete-command" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-drop-all-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-distribute-table" "s2-router-select" "s1-commit" "s1-select-count"

# permutations - real-time SELECT first
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-real-time-select" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-real-time-select" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-real-time-select" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-real-time-select" "s2-distribute-table" "s1-commit" "s1-select-count"

# permutations - real-time SELECT second
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-real-time-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-real-time-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-real-time-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-real-time-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-real-time-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-real-time-select" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-distribute-table" "s2-real-time-select" "s1-commit" "s1-select-count"

# permutations - task-tracker SELECT first
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-task-tracker-select" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-task-tracker-select" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-task-tracker-select" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-task-tracker-select" "s2-distribute-table" "s1-commit" "s1-select-count"

# permutations - task-tracker SELECT second
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-task-tracker-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-task-tracker-select" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-task-tracker-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-task-tracker-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-task-tracker-select" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-task-tracker-select" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s1-distribute-table" "s2-task-tracker-select" "s1-commit" "s1-select-count"
