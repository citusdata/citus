# remove one of the nodes for the purpose of the test
setup
{	
	SELECT master_remove_node('localhost', 57638);
	
    CREATE TABLE test_reference_table (test_id integer);
    SELECT create_reference_table('test_reference_table');
}

# ensure that both nodes exists for the remaining of the isolation tests
teardown
{
	DROP TABLE test_reference_table;
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57637);
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);
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

step "s2-begin"
{
    BEGIN;
}

step "s2-copy-to-reference-table" 
{
	COPY test_reference_table FROM PROGRAM 'echo "1\n2\n3\n4\n5"';
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


# verify that copy gets the invalidation and re-builts its metadata cache
# note that we need to run the same test twice to ensure that metadata is cached
# otherwise the test would be useless since the cache would be empty and the 
# metadata data is gathered from the tables directly
permutation "s1-begin" "s1-add-second-worker" "s2-begin" "s2-copy-to-reference-table" "s1-commit" "s2-commit" "s2-print-content" "s1-remove-second-worker" "s1-begin" "s1-add-second-worker" "s2-begin" "s2-copy-to-reference-table" "s1-commit" "s2-commit" "s2-print-content"
