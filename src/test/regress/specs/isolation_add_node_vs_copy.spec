setup
{
	truncate pg_dist_shard_placement;
	truncate pg_dist_shard;
	truncate pg_dist_partition;
	truncate pg_dist_colocation;
	truncate pg_dist_node;
	
	SELECT master_add_node('localhost', 57637);
	
    CREATE TABLE test_reference_table (test_id integer);
    SELECT create_reference_table('test_reference_table');
}

teardown
{
    DROP TABLE IF EXISTS test_reference_table CASCADE;

	SELECT master_add_node('localhost', 57637);
	SELECT master_add_node('localhost', 57638);
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-add-second-worker"
{
   	SELECT master_add_node('localhost', 57638);
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
	SELECT run_command_on_placements('test_reference_table', 'select count(*) from %s');
}

# verify that copy gets the invalidation and re-builts its metadata cache
# note that we need to run the same test twice to ensure that metadata is cached
# otherwise the test would be useless since the cache would be empty and the 
# metadata data is gathered from the tables directly
permutation "s1-begin" "s1-add-second-worker" "s2-begin" "s2-copy-to-reference-table" "s1-commit" "s2-commit" "s2-print-content" "s1-remove-second-worker" "s1-begin" "s1-add-second-worker" "s2-begin" "s2-copy-to-reference-table" "s1-commit" "s2-commit" "s2-print-content"
