# the test expects to have zero nodes in pg_dist_node at the beginning
# add single one of the nodes for the purpose of the test
setup
{
    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57637);
}

# ensure that both nodes exists for the remaining of the isolation tests
teardown
{
    -- schema drops are not cascaded
    SELECT run_command_on_workers($$DROP SCHEMA IF EXISTS myschema CASCADE;$$);
    DROP SCHEMA IF EXISTS myschema CASCADE;

    RESET search_path;
    DROP TABLE IF EXISTS t1 CASCADE;

	SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-add-worker"
{
	SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);
}

step "s1-commit"
{
    COMMIT;
}

# printing in session 1 adds the worker node, this makes we are sure we count the objects
# on that node as well
step "s1-print-distributed-objects"
{
    SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);

    -- print if the schema has been created
    SELECT count(*) FROM pg_namespace where nspname = 'myschema';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_namespace where nspname = 'myschema';$$);

    SELECT master_remove_node('localhost', 57638);
}

session "s2"

step "s2-public-schema"
{
    SET search_path TO public;
}

step "s2-create-schema"
{
    CREATE SCHEMA myschema;
    SET search_path TO myschema;
}

step "s2-create-table"
{
	CREATE TABLE t1 (a int, b int);
    -- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('t1', 'a');
}

step "s2-begin"
{
	BEGIN;
}

step "s2-commit"
{
	COMMIT;
}

# prints from session 2 are run at the end when the worker has already been added by the
# test
step "s2-print-distributed-objects"
{
    -- print if the schema has been created
    SELECT count(*) FROM pg_namespace where nspname = 'myschema';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_namespace where nspname = 'myschema';$$);
}

# schema only tests
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-public-schema" "s2-create-table" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-create-schema" "s2-create-table" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-create-schema" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-create-schema" "s2-create-table" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects"
