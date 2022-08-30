// the test expects to have zero nodes in pg_dist_node at the beginning
// add single one of the nodes for the purpose of the test
setup
{
    CREATE OR REPLACE FUNCTION public.wait_until_metadata_sync(timeout INTEGER DEFAULT 15000)
    RETURNS void
    LANGUAGE C STRICT
    AS 'citus';

    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
    SELECT 1 FROM master_add_node('localhost', 57637);
}

// ensure that both nodes exists for the remaining of the isolation tests
teardown
{
    SELECT 1 FROM master_add_node('localhost', 57637);
    SELECT 1 FROM master_add_node('localhost', 57638);

    RESET search_path;
    DROP SCHEMA IF EXISTS col_schema CASCADE;
    DROP TABLE IF EXISTS t1 CASCADE;
    DROP TABLE IF EXISTS t2 CASCADE;
    DROP TABLE IF EXISTS t3 CASCADE;
    DROP TYPE IF EXISTS tt1 CASCADE;
    DROP FUNCTION IF EXISTS add(INT,INT) CASCADE;

    SELECT master_remove_node(nodename, nodeport) FROM pg_dist_node;
}

session "s1"

step "s1-begin"
{
    BEGIN;
}

step "s1-add-worker"
{
    SELECT 1 FROM master_add_node('localhost', 57638);
}

step "s1-commit"
{
    COMMIT;
}

// printing in session 1 adds the worker node, this makes we are sure we count the objects
// on that node as well. After counting objects is done we remove the node again.
step "s1-print-distributed-objects"
{
    SELECT 1 FROM master_add_node('localhost', 57638);

    -- print an overview of all distributed objects
    SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM pg_catalog.pg_dist_object ORDER BY 1;

    -- print if the schema has been created
    SELECT count(*) FROM pg_namespace where nspname = 'myschema';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_namespace where nspname = 'myschema';$$);

    -- print if the type has been created
    SELECT count(*) FROM pg_type where typname = 'tt1';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_type where typname = 'tt1';$$);

    -- print if the function has been created
    SELECT count(*) FROM pg_proc WHERE proname='add';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='add';$$);

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

step "s2-create-type"
{
    CREATE TYPE tt1 AS (a int, b int);
}

step "s2-create-table-with-type"
{
    CREATE TABLE t1 (a int, b tt1);
    -- session needs to have replication factor set to 1, can't do in setup
    SET citus.shard_replication_factor TO 1;
    SELECT create_distributed_table('t1', 'a');
}

step "s2-distribute-function"
{
    CREATE OR REPLACE FUNCTION add (INT,INT) RETURNS INT AS $$ SELECT $1 + $2 $$ LANGUAGE SQL;
    SELECT create_distributed_function('add(INT,INT)', '$1');
}

step "s2-begin"
{
    BEGIN;
}

step "s2-commit"
{
    COMMIT;
}

step "s2-create-table-for-colocation"
{
    CREATE SCHEMA col_schema;
    CREATE TABLE col_schema.col_tbl (a INT, b INT);
    SELECT create_distributed_table('col_schema.col_tbl', 'a');
}

// prints from session 2 are run at the end when the worker has already been added by the
// test
step "s2-print-distributed-objects"
{
    -- print an overview of all distributed objects
    SELECT pg_identify_object_as_address(classid, objid, objsubid) FROM pg_catalog.pg_dist_object ORDER BY 1;

    -- print if the schema has been created
    SELECT count(*) FROM pg_namespace where nspname = 'myschema';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_namespace where nspname = 'myschema';$$);

    -- print if the type has been created
    SELECT count(*) FROM pg_type where typname = 'tt1';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_type where typname = 'tt1';$$);

    -- print if the function has been created
    SELECT count(*) FROM pg_proc WHERE proname='add';
    SELECT run_command_on_workers($$SELECT count(*) FROM pg_proc WHERE proname='add';$$);
}


session "s3"

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

step "s3-wait-for-metadata-sync"
{
    SELECT public.wait_until_metadata_sync(5000);
}

step "s3-create-schema2"
{
    CREATE SCHEMA myschema2;
    SET search_path TO myschema2;
}


step "s3-begin"
{
    BEGIN;
}

step "s3-commit"
{
    COMMIT;
}

step "s3-drop-coordinator-schemas"
{
    -- schema drops are not cascaded
    -- and cannot be dropped in a single
    -- transaction in teardown
    -- because it'd self-deadlock
    -- instead we drop the schemas
    -- at the end of the permutations
    DROP SCHEMA IF EXISTS myschema CASCADE;
    DROP SCHEMA IF EXISTS myschema2 CASCADE;
}


// schema only tests
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-public-schema" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-public-schema" "s2-create-table" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-create-schema" "s1-commit" "s2-create-table" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s1-add-worker" "s2-create-schema" "s1-commit" "s2-create-table" "s2-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-create-schema" "s1-add-worker" "s2-create-table" "s2-commit" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"

// concurrency tests with multi schema distribution
permutation "s1-print-distributed-objects" "s2-create-schema" "s1-begin" "s2-begin" "s1-add-worker" "s2-create-table" "s1-commit" "s2-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s2-create-table-for-colocation" "s1-add-worker" "s2-create-schema" "s2-begin" "s3-begin" "s3-use-schema" "s2-create-table" "s3-create-table" "s2-commit" "s3-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s3-begin" "s1-add-worker" "s2-create-schema" "s3-create-schema2" "s1-commit" "s2-create-table" "s2-commit" "s3-create-table" "s3-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"

// type and schema tests
permutation "s1-print-distributed-objects" "s1-begin" "s1-add-worker" "s2-public-schema" "s2-create-type" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-public-schema" "s2-create-type" "s1-add-worker" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s1-begin" "s2-begin" "s2-create-schema" "s2-create-type" "s2-create-table-with-type" "s1-add-worker" "s2-commit" "s1-commit" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"

// distributed function tests
// isolation tests are not very simple psql, so trigger NOTIFY reliably for
// s3-wait-for-metadata-sync step, we do "s2-begin" followed directly by
// "s2-commit", because "COMMIT"  syncs the messages

permutation "s1-print-distributed-objects" "s2-create-table-for-colocation" "s1-begin" "s1-add-worker" "s2-public-schema" "s2-distribute-function" "s1-commit" "s2-begin" "s2-commit"  "s3-wait-for-metadata-sync" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
permutation "s1-print-distributed-objects" "s2-create-table-for-colocation" "s1-begin" "s2-public-schema" "s2-distribute-function" "s2-begin" "s2-commit" "s3-wait-for-metadata-sync" "s1-add-worker" "s1-commit" "s3-wait-for-metadata-sync" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"

// we cannot run the following operations concurrently
// the problem is that NOTIFY event doesn't (reliably) happen before COMMIT
// so we have to commit s2 before s1 starts
permutation "s1-print-distributed-objects" "s2-create-table-for-colocation" "s2-begin" "s2-create-schema" "s2-distribute-function" "s2-commit" "s3-wait-for-metadata-sync" "s1-begin" "s1-add-worker" "s1-commit" "s3-wait-for-metadata-sync" "s2-print-distributed-objects" "s3-drop-coordinator-schemas"
