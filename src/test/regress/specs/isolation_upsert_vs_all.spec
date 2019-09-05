#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create range distributed table to test behavior of UPSERT in concurrent operations
setup
{
	SELECT citus_internal.replace_isolation_tester_func();
	SELECT citus_internal.refresh_isolation_tester_prepared_statement();

	SET citus.shard_replication_factor TO 1;
	CREATE TABLE upsert_hash(id integer PRIMARY KEY, data text);
	SELECT create_distributed_table('upsert_hash', 'id');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS upsert_hash CASCADE;

	SELECT citus_internal.restore_isolation_tester_func();
}

# session 1
session "s1"
step "s1-initialize" { COPY upsert_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-upsert" { INSERT INTO upsert_hash VALUES(4, 'k') ON CONFLICT(id) DO UPDATE SET data = 'k'; }
step "s1-update" { UPDATE upsert_hash SET data = 'l' WHERE id = 4; }
step "s1-delete" { DELETE FROM upsert_hash WHERE id = 4; }
step "s1-truncate" { TRUNCATE upsert_hash; }
step "s1-drop" { DROP TABLE upsert_hash; }
step "s1-ddl-create-index" { CREATE INDEX upsert_hash_index ON upsert_hash(id); }
step "s1-ddl-drop-index" { DROP INDEX upsert_hash_index; }
step "s1-ddl-add-column" { ALTER TABLE upsert_hash ADD new_column int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE upsert_hash DROP new_column; }
step "s1-ddl-rename-column" { ALTER TABLE upsert_hash RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('upsert_hash'); }
step "s1-master-modify-multiple-shards" { DELETE FROM upsert_hash; }
step "s1-create-non-distributed-table" { CREATE TABLE upsert_hash(id integer PRIMARY KEY, data text); }
step "s1-distribute-table" { SELECT create_distributed_table('upsert_hash', 'id'); }
step "s1-select-count" { SELECT COUNT(*) FROM upsert_hash; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''upsert_hash%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''upsert_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-upsert" { INSERT INTO upsert_hash VALUES(4, 'k') ON CONFLICT(id) DO UPDATE SET data = 'k'; }
step "s2-update" { UPDATE upsert_hash SET data = 'l' WHERE id = 4; }
step "s2-delete" { DELETE FROM upsert_hash WHERE id = 4; }
step "s2-truncate" { TRUNCATE upsert_hash; }
step "s2-drop" { DROP TABLE upsert_hash; }
step "s2-ddl-create-index" { CREATE INDEX upsert_hash_index ON upsert_hash(id); }
step "s2-ddl-drop-index" { DROP INDEX upsert_hash_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY upsert_hash_index ON upsert_hash(id); }
step "s2-ddl-add-column" { ALTER TABLE upsert_hash ADD new_column int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE upsert_hash DROP new_column; }
step "s2-ddl-rename-column" { ALTER TABLE upsert_hash RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('upsert_hash'); }
step "s2-master-modify-multiple-shards" { DELETE FROM upsert_hash; }
step "s2-distribute-table" { SELECT create_distributed_table('upsert_hash', 'id'); }
step "s2-commit" { COMMIT; }

# permutations - UPSERT vs UPSERT
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"

# permutations - UPSERT first
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-update" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-delete" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-truncate" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-drop" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-upsert" "s2-ddl-drop-index" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-upsert" "s2-ddl-create-index-concurrently" "s1-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-upsert" "s2-ddl-drop-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-table-size" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-upsert" "s2-distribute-table" "s1-commit" "s2-commit" "s1-select-count"

# permutations - UPSERT second
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-update" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-delete" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-truncate" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-drop" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-ddl-create-index" "s1-begin" "s2-begin" "s1-ddl-drop-index" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-ddl-add-column" "s1-begin" "s2-begin" "s1-ddl-drop-column" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-upsert" "s1-commit" "s2-commit" "s1-select-count"
