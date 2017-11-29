# the test expects to have zero nodes in pg_dist_node at the beginning
# add single one of the nodes for the purpose of the test
setup
{	
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57637);
	
	CREATE TABLE test_reference_table (test_id integer);
	SELECT create_reference_table('test_reference_table');
}

# ensure that both nodes exists for the remaining of the isolation tests
teardown
{
	DROP TABLE test_reference_table;
	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-add-second-worker"
{
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);
}

step "s1-remove-second-worker"
{
	SELECT master_remove_node('localhost', 57638);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

# COPY accesses all shard/placement metadata, so should be enough for
# loading the cache
step "s2-load-metadata-cache"
{
	COPY test_reference_table FROM PROGRAM 'echo 1 && echo 2 && echo 3 && echo 4 && echo 5';
}

step "s2-copy-to-reference-table" 
{
	COPY test_reference_table FROM PROGRAM 'echo 1 && echo 2 && echo 3 && echo 4 && echo 5';
}

step "s2-insert-to-reference-table"
{
	INSERT INTO test_reference_table VALUES (6);
}

step "s2-ddl-on-reference-table"
{
	CREATE INDEX reference_index ON test_reference_table(test_id);
}

step "s2-begin"
{
	BEGIN;
}

step "s2-commit"
{
	COMMIT;
}

step "s2-print-content"
{
	SELECT 
		nodeport, success, result 
	FROM 
		run_command_on_placements('test_reference_table', 'select count(*) from %s')
	ORDER BY
		nodeport;
}

step "s2-print-index-count"
{
	SELECT 
		nodeport, success, result 
	FROM 
		run_command_on_placements('test_reference_table', 'select count(*) from pg_indexes WHERE tablename = ''%s''')
	ORDER BY
		nodeport;
}

# verify that copy/insert gets the invalidation and re-builts its metadata cache
# note that we need to run  "s1-load-metadata-cache" and "s2-load-metadata-cache" 
# to ensure that metadata is cached otherwise the test would be useless since 
# the cache would be empty and the metadata data is gathered from the tables directly
permutation "s2-load-metadata-cache" "s1-begin" "s1-add-second-worker" "s2-copy-to-reference-table" "s1-commit" "s2-print-content"
permutation "s2-load-metadata-cache" "s2-begin" "s2-copy-to-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-content"
permutation "s2-load-metadata-cache" "s1-begin" "s1-add-second-worker" "s2-insert-to-reference-table" "s1-commit" "s2-print-content"
permutation "s2-load-metadata-cache" "s2-begin" "s2-insert-to-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-content"
permutation "s2-load-metadata-cache" "s1-begin" "s1-add-second-worker" "s2-ddl-on-reference-table" "s1-commit" "s2-print-index-count"
permutation "s2-load-metadata-cache" "s2-begin" "s2-ddl-on-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-index-count"


# same tests without loading the cache
permutation "s1-begin" "s1-add-second-worker" "s2-copy-to-reference-table" "s1-commit" "s2-print-content"
permutation "s2-begin" "s2-copy-to-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-content"
permutation "s1-begin" "s1-add-second-worker" "s2-insert-to-reference-table" "s1-commit" "s2-print-content"
permutation "s2-begin" "s2-insert-to-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-content"
permutation "s1-begin" "s1-add-second-worker" "s2-ddl-on-reference-table" "s1-commit" "s2-print-index-count"
permutation "s2-begin" "s2-ddl-on-reference-table" "s1-add-second-worker" "s2-commit" "s2-print-index-count"
