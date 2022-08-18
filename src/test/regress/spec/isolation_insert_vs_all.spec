//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create range distributed table to test behavior of INSERT in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE insert_hash(id integer, data text);
	SELECT create_distributed_table('insert_hash', 'id');
}

// drop distributed table
teardown
{
	DROP TABLE IF EXISTS insert_hash CASCADE;
}

// session 1
session "s1"
step "s1-initialize" { COPY insert_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-insert" { INSERT INTO insert_hash VALUES(7, 'k'); }
step "s1-insert-multi-row" { INSERT INTO insert_hash VALUES(7, 'k'), (8, 'l'), (9, 'm'); }
step "s1-insert-select" { INSERT INTO insert_hash SELECT * FROM insert_hash; }
step "s1-update" { UPDATE insert_hash SET data = 'l' WHERE id = 4; }
step "s1-delete" { DELETE FROM insert_hash WHERE id = 4; }
step "s1-truncate" { TRUNCATE insert_hash; }
step "s1-drop" { DROP TABLE insert_hash; }
step "s1-ddl-create-index" { CREATE INDEX insert_hash_index ON insert_hash(id); }
step "s1-ddl-drop-index" { DROP INDEX insert_hash_index; }
step "s1-ddl-add-column" { ALTER TABLE insert_hash ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE insert_hash DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE insert_hash RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('insert_hash'); }
step "s1-master-modify-multiple-shards" { DELETE FROM insert_hash; }
step "s1-create-non-distributed-table" { CREATE TABLE insert_hash(id integer, data text); COPY insert_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-distribute-table" { SELECT create_distributed_table('insert_hash', 'id'); }
step "s1-select-count" { SELECT COUNT(*) FROM insert_hash; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''insert_hash\_%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''insert_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

// session 2
session "s2"
step "s2-insert" { INSERT INTO insert_hash VALUES(7, 'k'); }
step "s2-insert-multi-row" { INSERT INTO insert_hash VALUES(7, 'k'), (8, 'l'), (9, 'm'); }
step "s2-insert-select" { INSERT INTO insert_hash SELECT * FROM insert_hash; }
step "s2-update" { UPDATE insert_hash SET data = 'l' WHERE id = 4; }
step "s2-delete" { DELETE FROM insert_hash WHERE id = 4; }
step "s2-truncate" { TRUNCATE insert_hash; }
step "s2-drop" { DROP TABLE insert_hash; }
step "s2-ddl-create-index" { CREATE INDEX insert_hash_index ON insert_hash(id); }
step "s2-ddl-drop-index" { DROP INDEX insert_hash_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY insert_hash_index ON insert_hash(id); }
step "s2-ddl-add-column" { ALTER TABLE insert_hash ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE insert_hash DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE insert_hash RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('insert_hash'); }
step "s2-master-modify-multiple-shards" { DELETE FROM insert_hash; }
step "s2-distribute-table" { SELECT create_distributed_table('insert_hash', 'id'); }
// We use this as a way to wait for s2-ddl-create-index-concurrently to
// complete. We know it can complete after s1-commit has succeeded, this way we
// make sure no other query is run over session s1 before that happens.
step "s2-empty" {}

// permutations - INSERT vs INSERT
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-insert-multi-row" "s1-commit" "s1-select-count"

// permutations - INSERT first
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-insert" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-insert" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-insert" "s2-distribute-table" "s1-commit" "s1-select-count"

// permutations - INSERT second
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-insert" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-insert" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-insert" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-insert" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-insert" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-insert" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-distribute-table" "s2-insert" "s1-commit" "s1-select-count"

// permutations - multi row INSERT first
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-insert-select" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-update" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-delete" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-truncate" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-drop" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-ddl-create-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-insert-multi-row" "s2-ddl-drop-index" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-ddl-add-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-insert-multi-row" "s2-ddl-drop-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-ddl-rename-column" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-table-size" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-master-modify-multiple-shards" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-insert-multi-row" "s2-distribute-table" "s1-commit" "s1-select-count"

// permutations - multi row INSERT second
permutation "s1-initialize" "s1-begin" "s1-insert-select" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-update" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-truncate" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-drop" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-insert-multi-row" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s1-ddl-drop-index" "s2-insert-multi-row" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-insert-multi-row" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s1-ddl-drop-column" "s2-insert-multi-row" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-insert-multi-row" "s1-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-insert-multi-row" "s1-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-distribute-table" "s2-insert-multi-row" "s1-commit" "s1-select-count"
