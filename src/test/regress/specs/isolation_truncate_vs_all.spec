#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create range distributed table to test behavior of TRUNCATE in concurrent operations
setup
{
	SELECT citus_internal.replace_isolation_tester_func();
	SELECT citus_internal.refresh_isolation_tester_prepared_statement();

	SET citus.shard_replication_factor TO 1;
	CREATE TABLE truncate_append(id integer, data text);
	SELECT create_distributed_table('truncate_append', 'id', 'append');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS truncate_append CASCADE;

	SELECT citus_internal.restore_isolation_tester_func();
}

# session 1
session "s1"
step "s1-initialize" { COPY truncate_append FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-truncate" { TRUNCATE truncate_append; }
step "s1-drop" { DROP TABLE truncate_append; }
step "s1-ddl-create-index" { CREATE INDEX truncate_append_index ON truncate_append(id); }
step "s1-ddl-drop-index" { DROP INDEX truncate_append_index; }
step "s1-ddl-add-column" { ALTER TABLE truncate_append ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE truncate_append DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE truncate_append RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('truncate_append'); }
step "s1-master-modify-multiple-shards" { DELETE FROM truncate_append; }
step "s1-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM truncate_append WHERE id <= 4;'); }
step "s1-master-drop-all-shards" { SELECT master_drop_all_shards('truncate_append'::regclass, 'public', 'truncate_append'); }
step "s1-create-non-distributed-table" { CREATE TABLE truncate_append(id integer, data text); }
step "s1-distribute-table" { SELECT create_distributed_table('truncate_append', 'id', 'append'); }
step "s1-select-count" { SELECT COUNT(*) FROM truncate_append; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''truncate_append%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''truncate_append%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-truncate" { TRUNCATE truncate_append; }
step "s2-drop" { DROP TABLE truncate_append; }
step "s2-ddl-create-index" { CREATE INDEX truncate_append_index ON truncate_append(id); }
step "s2-ddl-drop-index" { DROP INDEX truncate_append_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY truncate_append_index ON truncate_append(id); }
step "s2-ddl-add-column" { ALTER TABLE truncate_append ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE truncate_append DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE truncate_append RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('truncate_append'); }
step "s2-master-modify-multiple-shards" { DELETE FROM truncate_append; }
step "s2-master-apply-delete-command" { SELECT master_apply_delete_command('DELETE FROM truncate_append WHERE id <= 4;'); }
step "s2-master-drop-all-shards" { SELECT master_drop_all_shards('truncate_append'::regclass, 'public', 'truncate_append'); }
step "s2-create-non-distributed-table" { CREATE TABLE truncate_append(id integer, data text); }
step "s2-distribute-table" { SELECT create_distributed_table('truncate_append', 'id', 'append'); }
step "s2-select" { SELECT * FROM truncate_append ORDER BY 1, 2; }
step "s2-commit" { COMMIT; }

# permutations - TRUNCATE vs TRUNCATE
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"

# permutations - TRUNCATE first
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-drop" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-truncate" "s2-ddl-drop-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-truncate" "s2-ddl-drop-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-table-size" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-master-apply-delete-command" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-master-drop-all-shards" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s2-begin" "s1-truncate" "s2-distribute-table" "s1-commit" "s2-commit" "s1-select-count"

# permutations - TRUNCATE second
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-drop" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-ddl-drop-index" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-ddl-drop-column" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-apply-delete-command" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-drop-all-shards" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-begin" "s2-begin" "s1-distribute-table" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
