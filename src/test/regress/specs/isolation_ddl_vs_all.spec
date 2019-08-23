#
# How we organize this isolation test spec, is explained at README.md file in this directory.
#

# create range distributed table to test behavior of DDL in concurrent operations
setup
{
	SELECT citus_internal.replace_isolation_tester_func();
	SELECT citus_internal.refresh_isolation_tester_prepared_statement();

	SET citus.shard_replication_factor TO 1;
	CREATE TABLE ddl_hash(id integer, data text);
	SELECT create_distributed_table('ddl_hash', 'id');
}

# drop distributed table
teardown
{
	DROP TABLE IF EXISTS ddl_hash CASCADE;

	SELECT citus_internal.restore_isolation_tester_func();
}

# session 1
session "s1"
step "s1-initialize" { COPY ddl_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-ddl-create-index" { CREATE INDEX ddl_hash_index ON ddl_hash(id); }
step "s1-ddl-drop-index" { DROP INDEX ddl_hash_index; }
step "s1-ddl-add-column" { ALTER TABLE ddl_hash ADD new_column_1 int DEFAULT 0; }
step "s1-ddl-drop-column" { ALTER TABLE ddl_hash DROP new_column_2; }
step "s1-ddl-rename-column" { ALTER TABLE ddl_hash RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('ddl_hash'); }
step "s1-master-modify-multiple-shards" { DELETE FROM ddl_hash; }
step "s1-drop" { DROP TABLE ddl_hash; }
step "s1-create-non-distributed-table" { CREATE TABLE ddl_hash(id integer, data text); COPY ddl_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-distribute-table" { SELECT create_distributed_table('ddl_hash', 'id'); }
step "s1-select-count" { SELECT COUNT(*) FROM ddl_hash; }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''ddl_hash%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''ddl_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

# session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-ddl-create-index" { CREATE INDEX ddl_hash_index ON ddl_hash(id); }
step "s2-ddl-drop-index" { DROP INDEX ddl_hash_index; }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY ddl_hash_index ON ddl_hash(id); }
step "s2-ddl-add-column" { ALTER TABLE ddl_hash ADD new_column_2 int DEFAULT 0; }
step "s2-ddl-drop-column" { ALTER TABLE ddl_hash DROP new_column_1; }
step "s2-ddl-rename-column" { ALTER TABLE ddl_hash RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('ddl_hash'); }
step "s2-master-modify-multiple-shards" { DELETE FROM ddl_hash; }
step "s2-create-non-distributed-table" { CREATE TABLE ddl_hash(id integer, data text); COPY ddl_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s2-distribute-table" { SELECT create_distributed_table('ddl_hash', 'id'); }
step "s2-select" { SELECT * FROM ddl_hash ORDER BY 1, 2; }
step "s2-commit" { COMMIT; }

# permutations - DDL vs DDL
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-indexes" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-indexes" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"

# permutations - DDL first
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-table-size" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-indexes"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-table-size" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-columns"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-table-size" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-columns"

# permutations - DDL second
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"

permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-distribute-table" "s2-ddl-create-index-concurrently" "s1-commit" "s1-show-indexes"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
