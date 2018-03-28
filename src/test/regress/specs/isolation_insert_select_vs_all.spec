#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create range distributed table to test behavior of INSERT/SELECT in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE insert_of_insert_select_hash(id integer, data text);
	SELECT create_distributed_table('insert_of_insert_select_hash', 'id');
	CREATE TABLE select_of_insert_select_hash(id integer, data text);
	SELECT create_distributed_table('select_of_insert_select_hash', 'id');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS insert_of_insert_select_hash CASCADE;
	DROP TABLE IF EXISTS select_of_insert_select_hash CASCADE;
}

# session 1
session "s1"
step "s1-initialize"
{
	COPY insert_of_insert_select_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV;
	COPY select_of_insert_select_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV;
	COPY insert_of_insert_select_hash FROM PROGRAM 'echo 5, a && echo 6, b && echo 7, c && echo 8, d && echo 9, e' WITH CSV;
	COPY select_of_insert_select_hash FROM PROGRAM 'echo 5, a && echo 6, b && echo 7, c && echo 8, d && echo 9, e' WITH CSV;
}
step "s1-begin" { BEGIN; }
step "s1-insert-select" { INSERT INTO insert_of_insert_select_hash SELECT * FROM select_of_insert_select_hash ORDER BY 1, 2 LIMIT 5;; }
step "s1-update-on-inserted" { UPDATE insert_of_insert_select_hash SET data = 'l' WHERE id = 4; }
step "s1-delete-on-inserted" { DELETE FROM insert_of_insert_select_hash WHERE id = 4; }
step "s1-truncate-on-inserted" { TRUNCATE insert_of_insert_select_hash; }
step "s1-drop-on-inserted" { DROP TABLE insert_of_insert_select_hash; }
step "s1-ddl-create-index-on-inserted" { CREATE INDEX insert_of_insert_select_hash_index ON insert_of_insert_select_hash(id); }
step "s1-ddl-drop-index-on-inserted" { DROP INDEX insert_of_insert_select_hash_index; }
step "s1-ddl-add-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash DROP new_column; }
step "s1-ddl-rename-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash RENAME data TO new_column; }
step "s1-table-size-on-inserted" { SELECT citus_total_relation_size('insert_of_insert_select_hash'); }
step "s1-master-modify-multiple-shards-on-inserted" { SELECT master_modify_multiple_shards('DELETE FROM insert_of_insert_select_hash;'); }
step "s1-master-drop-all-shards-on-inserted" { SELECT master_drop_all_shards('insert_of_insert_select_hash'::regclass, 'public', 'insert_of_insert_select_hash'); }
step "s1-create-non-distributed-table-on-inserted" { CREATE TABLE insert_of_insert_select_hash(id integer, data text); }
step "s1-distribute-table-on-inserted" { SELECT create_distributed_table('insert_of_insert_select_hash', 'id'); }
step "s1-show-indexes-inserted" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''insert_of_insert_select_hash%'''); }
step "s1-show-columns-inserted" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''insert_of_insert_select_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-update-on-selected" { UPDATE select_of_insert_select_hash SET data = 'l' WHERE id = 4; }
step "s1-delete-on-selected" { DELETE FROM select_of_insert_select_hash WHERE id = 4; }
step "s1-truncate-on-selected" { TRUNCATE select_of_insert_select_hash; }
step "s1-drop-on-selected" { DROP TABLE select_of_insert_select_hash; }
step "s1-ddl-create-index-on-selected" { CREATE INDEX select_of_insert_select_hash_index ON select_of_insert_select_hash(id); }
step "s1-ddl-drop-index-on-selected" { DROP INDEX select_of_insert_select_hash_index; }
step "s1-ddl-add-column-on-selected" { ALTER TABLE select_of_insert_select_hash ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column-on-selected" { ALTER TABLE select_of_insert_select_hash DROP new_column; }
step "s1-ddl-rename-column-on-selected" { ALTER TABLE select_of_insert_select_hash RENAME data TO new_column; }
step "s1-table-size-on-selected" { SELECT citus_total_relation_size('select_of_insert_select_hash'); }
step "s1-master-modify-multiple-shards-on-selected" { SELECT master_modify_multiple_shards('DELETE FROM select_of_insert_select_hash;'); }
step "s1-master-drop-all-shards-on-selected" { SELECT master_drop_all_shards('select_of_insert_select_hash'::regclass, 'public', 'select_of_insert_select_hash'); }
step "s1-create-non-distributed-table-on-selected" { CREATE TABLE select_of_insert_select_hash(id integer, data text); }
step "s1-distribute-table-on-selected" { SELECT create_distributed_table('select_of_insert_select_hash', 'id'); }
step "s1-show-indexes-selected" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''select_of_insert_select_hash%'''); }
step "s1-show-columns-selected" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''select_of_insert_select_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-select-count" { SELECT COUNT(*) FROM select_of_insert_select_hash; }
step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-insert-select" { INSERT INTO insert_of_insert_select_hash SELECT * FROM select_of_insert_select_hash ORDER BY 1, 2 LIMIT 5; }
step "s2-update-on-inserted" { UPDATE insert_of_insert_select_hash SET data = 'l' WHERE id = 4; }
step "s2-delete-on-inserted" { DELETE FROM insert_of_insert_select_hash WHERE id = 4; }
step "s2-truncate-on-inserted" { TRUNCATE insert_of_insert_select_hash; }
step "s2-drop-on-inserted" { DROP TABLE insert_of_insert_select_hash; }
step "s2-ddl-create-index-on-inserted" { CREATE INDEX insert_of_insert_select_hash_index ON insert_of_insert_select_hash(id); }
step "s2-ddl-drop-index-on-inserted" { DROP INDEX insert_of_insert_select_hash_index; }
step "s2-ddl-create-index-concurrently-on-inserted" { CREATE INDEX CONCURRENTLY insert_of_insert_select_hash_index ON insert_of_insert_select_hash(id); }
step "s2-ddl-add-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash DROP new_column; }
step "s2-ddl-rename-column-on-inserted" { ALTER TABLE insert_of_insert_select_hash RENAME data TO new_column; }
step "s2-table-size-on-inserted" { SELECT citus_total_relation_size('insert_of_insert_select_hash'); }
step "s2-master-modify-multiple-shards-on-inserted" { SELECT master_modify_multiple_shards('DELETE FROM insert_of_insert_select_hash;'); }
step "s2-master-drop-all-shards-on-inserted" { SELECT master_drop_all_shards('insert_of_insert_select_hash'::regclass, 'public', 'insert_of_insert_select_hash'); }
step "s2-create-non-distributed-table-on-inserted" { CREATE TABLE insert_of_insert_select_hash(id integer, data text); }
step "s2-distribute-table-on-inserted" { SELECT create_distributed_table('insert_of_insert_select_hash', 'id'); }
step "s2-update-on-selected" { UPDATE select_of_insert_select_hash SET data = 'l' WHERE id = 4; }
step "s2-delete-on-selected" { DELETE FROM select_of_insert_select_hash WHERE id = 4; }
step "s2-truncate-on-selected" { TRUNCATE select_of_insert_select_hash; }
step "s2-drop-on-selected" { DROP TABLE select_of_insert_select_hash; }
step "s2-ddl-create-index-on-selected" { CREATE INDEX select_of_insert_select_hash_index ON select_of_insert_select_hash(id); }
step "s2-ddl-drop-index-on-selected" { DROP INDEX select_of_insert_select_hash_index; }
step "s2-ddl-create-index-concurrently-on-selected" { CREATE INDEX CONCURRENTLY select_of_insert_select_hash_index ON insert_of_insert_select_hash(id); }
step "s2-ddl-add-column-on-selected" { ALTER TABLE select_of_insert_select_hash ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column-on-selected" { ALTER TABLE select_of_insert_select_hash DROP new_column; }
step "s2-ddl-rename-column-on-selected" { ALTER TABLE select_of_insert_select_hash RENAME data TO new_column; }
step "s2-table-size-on-selected" { SELECT citus_total_relation_size('select_of_insert_select_hash'); }
step "s2-master-modify-multiple-shards-on-selected" { SELECT master_modify_multiple_shards('DELETE FROM select_of_insert_select_hash;'); }
step "s2-master-drop-all-shards-on-selected" { SELECT master_drop_all_shards('select_of_insert_select_hash'::regclass, 'public', 'select_of_insert_select_hash'); }
step "s2-create-non-distributed-table-on-selected" { CREATE TABLE select_of_insert_select_hash(id integer, data text); }
step "s2-distribute-table-on-selected" { SELECT create_distributed_table('select_of_insert_select_hash', 'id'); }

# permutations - INSERT/SELECT vs INSERT/SELECT
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-insert-select" "s1-commit" "s1-select-count"

# permutations - INSERT/SELECT first operation on INSERT side
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-update-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-delete-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-truncate-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-drop-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-create-index-on-inserted" "s1-commit" "s1-select-count" "s1-show-indexes-inserted"
permutation "s1-initialize" "s1-ddl-create-index-on-inserted" "s1-begin" "s1-insert-select" "s2-ddl-drop-index-on-inserted" "s1-commit" "s1-select-count" "s1-show-indexes-inserted"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-create-index-concurrently-on-inserted" "s1-commit" "s1-select-count" "s1-show-indexes-inserted"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-add-column-on-inserted" "s1-commit" "s1-select-count" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-ddl-add-column-on-inserted" "s1-begin" "s1-insert-select" "s2-ddl-drop-column-on-inserted" "s1-commit" "s1-select-count" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-rename-column-on-inserted" "s1-commit" "s1-select-count" "s1-show-columns-inserted" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-table-size-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-master-modify-multiple-shards-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-master-drop-all-shards-on-inserted" "s1-commit" "s1-select-count"
permutation "s1-drop-on-inserted" "s1-create-non-distributed-table-on-inserted" "s1-initialize" "s1-begin" "s1-insert-select" "s2-distribute-table-on-inserted" "s1-commit" "s1-select-count"

# permutations - INSERT/SELECT first operation on SELECT side
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-update-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-delete-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-truncate-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-drop-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-create-index-on-selected" "s1-commit" "s1-select-count" "s1-show-indexes-selected"
permutation "s1-initialize" "s1-ddl-create-index-on-selected" "s1-begin" "s1-insert-select" "s2-ddl-drop-index-on-selected" "s1-commit" "s1-select-count" "s1-show-indexes-selected"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-create-index-concurrently-on-selected" "s1-commit" "s1-select-count" "s1-show-indexes-selected"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-add-column-on-selected" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-ddl-add-column-on-selected" "s1-begin" "s1-insert-select" "s2-ddl-drop-column-on-selected" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-ddl-rename-column-on-selected" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-table-size-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-master-modify-multiple-shards-on-selected" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-master-drop-all-shards-on-selected" "s1-commit" "s1-select-count"
permutation "s1-drop-on-selected" "s1-create-non-distributed-table-on-selected" "s1-initialize" "s1-begin" "s1-insert-select" "s2-distribute-table-on-selected" "s1-commit" "s1-select-count"

# permutations - INSERT/SELECT second on INSERT side
permutation "s1-initialize" "s1-begin" "s1-update-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-indexes-inserted"
permutation "s1-initialize" "s1-ddl-create-index-on-inserted" "s1-begin" "s1-ddl-drop-index-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-indexes-inserted"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-ddl-add-column-on-inserted" "s1-begin" "s1-ddl-drop-column-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-inserted"
permutation "s1-initialize" "s1-begin" "s1-table-size-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-drop-all-shards-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-drop-on-inserted" "s1-create-non-distributed-table-on-inserted" "s1-initialize" "s1-begin" "s1-distribute-table-on-inserted" "s2-insert-select" "s1-commit" "s1-select-count"

# permutations - INSERT/SELECT second on SELECT side
permutation "s1-initialize" "s1-begin" "s1-update-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index-on-selected" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-indexes-selected"
permutation "s1-initialize" "s1-ddl-create-index-on-selected" "s1-begin" "s1-ddl-drop-index-on-selected" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-indexes-selected"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column-on-selected" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-ddl-add-column-on-selected" "s1-begin" "s1-ddl-drop-column-on-selected" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column-on-selected" "s2-insert-select" "s1-commit" "s1-select-count" "s1-show-columns-selected"
permutation "s1-initialize" "s1-begin" "s1-table-size-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-drop-all-shards-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-drop-on-selected" "s1-create-non-distributed-table-on-selected" "s1-initialize" "s1-begin" "s1-distribute-table-on-selected" "s2-insert-select" "s1-commit" "s1-select-count"
