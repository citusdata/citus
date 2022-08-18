//
// How we organize this isolation test spec, is explained at README.md file in this directory.
//

// create range distributed table to test behavior of DDL in concurrent operations
setup
{
	SET citus.shard_replication_factor TO 1;
	CREATE TABLE ddl_hash(id integer, data text);
	SELECT create_distributed_table('ddl_hash', 'id');
}

// drop distributed table
teardown
{
	DROP TABLE IF EXISTS ddl_hash CASCADE;
}

// session 1
session "s1"
step "s1-initialize" { COPY ddl_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-begin" { BEGIN; }
step "s1-ddl-create-index" { CREATE INDEX ddl_hash_index ON ddl_hash(id); }
step "s1-ddl-add-column" { ALTER TABLE ddl_hash ADD new_column_1 int DEFAULT 0; }
step "s1-ddl-rename-column" { ALTER TABLE ddl_hash RENAME data TO new_column; }
step "s1-table-size" { SELECT citus_total_relation_size('ddl_hash'); }
step "s1-master-modify-multiple-shards" { DELETE FROM ddl_hash; }
step "s1-drop" { DROP TABLE ddl_hash; }
step "s1-create-non-distributed-table" { CREATE TABLE ddl_hash(id integer, data text); COPY ddl_hash FROM PROGRAM 'echo 0, a && echo 1, b && echo 2, c && echo 3, d && echo 4, e' WITH CSV; }
step "s1-distribute-table" { SELECT create_distributed_table('ddl_hash', 'id'); }
step "s1-show-indexes" { SELECT run_command_on_workers('SELECT COUNT(*) FROM pg_indexes WHERE tablename LIKE ''ddl_hash\_%'''); }
step "s1-show-columns" { SELECT run_command_on_workers('SELECT column_name FROM information_schema.columns WHERE table_name LIKE ''ddl_hash%'' AND column_name = ''new_column'' ORDER BY 1 LIMIT 1'); }
step "s1-commit" { COMMIT; }

// session 2
session "s2"
step "s2-begin" { BEGIN; }
step "s2-ddl-create-index" { CREATE INDEX ddl_hash_index ON ddl_hash(id); }
step "s2-ddl-create-index-concurrently" { CREATE INDEX CONCURRENTLY ddl_hash_index ON ddl_hash(id); }
step "s2-ddl-add-column" { ALTER TABLE ddl_hash ADD new_column_2 int DEFAULT 0; }
step "s2-ddl-rename-column" { ALTER TABLE ddl_hash RENAME data TO new_column; }
step "s2-table-size" { SELECT citus_total_relation_size('ddl_hash'); }
step "s2-master-modify-multiple-shards" { DELETE FROM ddl_hash; }
step "s2-distribute-table" { SELECT create_distributed_table('ddl_hash', 'id'); }
step "s2-commit" { COMMIT; }
// This s2-empty step is used as a way to wait for the create index
// concurrently command to complete before running s1-show-xxx. This is only
// necessary for permutations where we don't run s2-commit, since that one
// implicitly takes on that same function.
step "s2-empty" {}

// permutations - DDL vs DDL
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-create-index" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-indexes" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-indexes" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-add-column" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-ddl-rename-column" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-show-columns" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"

// permutations - DDL first
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-table-size" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-create-index" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-indexes"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-table-size" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-add-column" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-columns"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-table-size" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-master-modify-multiple-shards" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-ddl-rename-column" "s2-distribute-table" "s1-commit" "s2-commit" "s1-show-columns"

// permutations - DDL second
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-create-index" "s1-commit" "s2-commit" "s1-show-indexes"

// We use s2-empty slightly differently for this permutation than in the rest
// of the permutations: We know create-index-concurrently doesn't have to wait
// for s1-commit here, but the isolationtester sometimes detects it temporarily
// as blocking. To get consistent test output we use a (*) marker to always
// show create index concurrently as blocking. Then right after we put
// s2-empty, to wait for it to complete.
permutation "s1-initialize" "s1-begin" "s1-table-size" "s2-ddl-create-index-concurrently"(*) "s2-empty" "s1-commit" "s1-show-indexes"
permutation "s1-initialize" "s1-begin" "s1-master-modify-multiple-shards" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-show-indexes"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s1-distribute-table" "s2-ddl-create-index-concurrently" "s1-commit" "s2-empty" "s1-show-indexes"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-add-column" "s1-commit" "s2-commit" "s1-show-columns"

permutation "s1-initialize" "s1-begin" "s2-begin" "s1-table-size" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-initialize" "s1-begin" "s2-begin" "s1-master-modify-multiple-shards" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
permutation "s1-drop" "s1-create-non-distributed-table" "s1-initialize" "s1-begin" "s2-begin" "s1-distribute-table" "s2-ddl-rename-column" "s1-commit" "s2-commit" "s1-show-columns"
