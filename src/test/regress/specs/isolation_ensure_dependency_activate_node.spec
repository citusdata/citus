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
    SELECT run_command_on_workers($$DROP SCHEMA IF EXISTS myschema2 CASCADE;$$);
    DROP SCHEMA IF EXISTS myschema2 CASCADE;

    RESET search_path;
    DROP TABLE IF EXISTS t1 CASCADE;
    DROP TABLE IF EXISTS t2 CASCADE;
    DROP TABLE IF EXISTS t3 CASCADE;

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
# on that node as well. After counting objects is done we remove the node again.
step "s1-print-distributed-objects"
{
    SELECT nodename, nodeport, isactive FROM master_add_node('localhost', 57638);

    -- print an overview of all distributed objects
    SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM citus.pg_dist_object ORDER BY 1;

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
    -- print an overview of all distributed objects
    SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM citus.pg_dist_object ORDER BY 1;

    -- print if the schema has been created
    SELECT count(*) FROM pg_namespace where nspname = 'myschema';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_namespace where nspname = 'myschema';$$);
}

session "s3"

step "s3-public-schema"
{
    SET search_path TO public;
}

step "s3-use-schema"
{
    SET search_path TO myschema;
}

step "s3-create-table"
{
	CREATE TABLE t2 (a int, b int);
    -- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('t2', 'a');
}

step "s3-begin"
{
	BEGIN;
}

step "s3-commit"
{
	COMMIT;
}

session "s4"

step "s4-public-schema"
{
    SET search_path TO public;
}

step "s4-use-schema"
{
    SET search_path TO myschema;
}

step "s4-create-schema2"
{
    CREATE SCHEMA myschema2;
    SET search_path TO myschema2;
}

step "s4-create-table"
{
	CREATE TABLE t3 (a int, b int);
    -- session needs to have replication factor set to 1, can't do in setup
	SET citus.shard_replication_factor TO 1;
	SELECT create_distributed_table('t3', 'a');
}

step "s4-begin"
{
	BEGIN;
}

step "s4-commit"
{
	COMMIT;
}

# schema only tests
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-public-schema" "s2-create-table" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-create-schema" "s2-create-table" "s1-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-create-schema" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-create-schema" "s2-create-table" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects"

# concurrency tests with multi schema distribution
permutation "s1-print-distributed-objects" "s2-create-schema" "s1-begin" "s2-begin" "s3-begin" "s1-add-worker" "s2-create-table" "s3-use-schema" "s3-create-table" "s1-commit" "s2-commit" "s3-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s2-create-schema" "s1-begin" "s2-begin" "s3-begin" "s4-begin" "s1-add-worker" "s2-create-table" "s3-use-schema" "s3-create-table" "s4-use-schema" "s4-create-table" "s1-commit" "s2-commit" "s3-commit" "s4-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-add-worker" "s2-create-schema" "s2-begin" "s3-begin" "s3-use-schema" "s2-create-table" "s3-create-table" "s2-commit" "s3-commit" "s2-print-distributed-objects"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s4-begin" "s1-add-worker" "s2-create-schema" "s4-create-schema2" "s2-create-table" "s4-create-table" "s1-commit" "s2-commit" "s4-commit" "s2-print-distributed-objects"
