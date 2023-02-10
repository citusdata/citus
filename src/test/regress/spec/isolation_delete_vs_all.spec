//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create range distributed table to test behavior of DELETE in concurrent operations
setup
{
    SET citus.shard_replication_factor TO 1;
    CREATE TABLE delete_hash(id integer, data text);
    SELECT create_distributed_table('delete_hash', 'id');
}

// drop distributed table
teardown
{
    DROP TABLE IF EXISTS delete_hash CASCADE;
}

// session 1
session "s1"
step "s1-initialize" { COPY delete_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-delete" { DELETE FROM delete_hash WHERE id = 4; }
step "s1-truncate" { TRUNCATE delete_hash; }
step "s1-drop" { DROP TABLE delete_hash; }
step "s1-ddl-create-index" { CREATE INDEX delete_hash_index ON delete_hash(id); }
step "s1-ddl-drop-index" { DROP INDEX delete_hash_index; }
step "s1-ddl-add-column" { ALTER TABLE delete_hash ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE delete_hash DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE delete_hash RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('delete_hash'); }
step "s1-create-non-distributed-table" { CREATE TABLE delete_hash(id integer, data text); COPY delete_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-distribute-table" { SELECT create_distributed_table('delete_hash', 'id'); }
step "s1-select-count" { SELECT COUNT(*) FROM delete_hash; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''delete_hash\_%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''delete_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

// session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-delete" { DELETE FROM delete_hash WHERE id = 4; }
step "s2-truncate" { TRUNCATE delete_hash; }
step "s2-drop" { DROP TABLE delete_hash; }
step "s2-ddl-create-index" { CREATE INDEX delete_hash_index ON delete_hash(id); }
step "s2-ddl-drop-index" { DROP INDEX delete_hash_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY delete_hash_index ON delete_hash(id); }
step "s2-ddl-add-column" { ALTER TABLE delete_hash ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE delete_hash DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE delete_hash RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('delete_hash'); }
step "s2-distribute-table" { SELECT create_distributed_table('delete_hash', 'id'); }
step "s2-commit" { COMMIT; }
// We use this as a way to wait for s2-ddl-create-index-concurrently to
// complete. We know it can complete after s1-commit has succeeded, this way we
// make sure no other query is run over session s1 before that happens.
step "s2-empty" {}

// permutations - DELETE vs DELETE
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"

// permutations - DELETE first
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-drop" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-delete" "s2-ddl-drop-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-delete" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-delete" "s2-ddl-drop-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-table-size" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-distribute-table" "s1-commit" "s2-commit" "s1-select-count"

// permutations - DELETE second
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-drop" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-delete" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-ddl-drop-index" "s2-delete" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-delete" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-ddl-drop-column" "s2-delete" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-delete" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"
