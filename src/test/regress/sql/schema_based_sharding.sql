CREATE SCHEMA regular_schema;
SET search_path TO regular_schema;

SET citus.next_shard_id TO 1920000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET client_min_messages TO WARNING;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

SET client_min_messages TO NOTICE;

-- Verify that the UDFs used to sync tenant schema metadata to workers
-- fail on NULL input.
SELECT citus_internal_insert_tenant_schema(NULL, 1);
SELECT citus_internal_insert_tenant_schema(1, NULL);
SELECT citus_internal_delete_tenant_schema(NULL);
SELECT citus_internal_set_tenant_schema_colocation_id(NULL, 1);
SELECT citus_internal_set_tenant_schema_colocation_id(1, NULL);

CREATE ROLE test_non_super_user WITH LOGIN;
ALTER ROLE test_non_super_user NOSUPERUSER;

\c - test_non_super_user

-- Verify that the UDFs used to sync tenant schema metadata to workers
-- fail when called via a non-superuser.
SELECT citus_internal_insert_tenant_schema(1, 1);
SELECT citus_internal_delete_tenant_schema(1);
SELECT citus_internal_set_tenant_schema_colocation_id(1, 1);

\c - postgres

DROP ROLE test_non_super_user;

SET search_path TO regular_schema;
SET citus.next_shard_id TO 1930000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

-- Verify that the UDFs used to sync tenant schema metadata to workers
-- fail when called via a superuser that is not allowed to modify metadata.
SELECT citus_internal_insert_tenant_schema(1, 1);
SELECT citus_internal_delete_tenant_schema(1);
SELECT citus_internal_set_tenant_schema_colocation_id(1, 1);

ALTER SYSTEM SET citus.enable_manual_metadata_changes_for_user TO 'postgres';
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

-- Verify that the UDFs used to sync tenant schema metadata to workers
-- fail on schema_id = InvalidOid
SELECT citus_internal_insert_tenant_schema(0, 1);
SELECT citus_internal_delete_tenant_schema(0);
SELECT citus_internal_set_tenant_schema_colocation_id(0, 1);

-- Verify that the UDFs used to sync tenant schema metadata to workers
-- fail on non-existing schema_id.
SELECT citus_internal_delete_tenant_schema(456456);
SELECT citus_internal_set_tenant_schema_colocation_id(456456, 1);

ALTER SYSTEM RESET citus.enable_manual_metadata_changes_for_user;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);

SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

CREATE TABLE regular_schema.test_table(a int, b text);

SET citus.enable_schema_based_sharding TO ON;

-- show that regular_schema doesn't show up in pg_dist_tenant_schema
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id::regnamespace::text = 'regular_schema';

-- empty tenant
CREATE SCHEMA "tenant\'_1";

-- non-empty tenant
CREATE SCHEMA "tenant\'_2";
CREATE TABLE "tenant\'_2".test_table(a int, b text);

-- empty tenant
CREATE SCHEMA "tenant\'_3";
CREATE TABLE "tenant\'_3".test_table(a int, b text);
DROP TABLE "tenant\'_3".test_table;

-- add a node after creating tenant schemas
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

ALTER SCHEMA "tenant\'_1" RENAME TO tenant_1;
ALTER SCHEMA "tenant\'_2" RENAME TO tenant_2;
ALTER SCHEMA "tenant\'_3" RENAME TO tenant_3;

-- verify that create_distributed_table() and others fail when called on tenant tables
SELECT create_distributed_table('tenant_2.test_table', 'a');
SELECT create_reference_table('tenant_2.test_table');
SELECT citus_add_local_table_to_metadata('tenant_2.test_table');

-- (on coordinator) verify that colocation id is not set for empty tenants
SELECT colocation_id IS NULL FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text IN ('tenant_1', 'tenant_3');

-- (on workers) verify that colocation id is not set for empty tenants
SELECT result FROM run_command_on_workers($$
    SELECT array_agg(colocation_id IS NULL) FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text IN ('tenant_1', 'tenant_3');
$$);

-- Verify that tenant_2.test_table is recorded in pg_dist_partition as a
-- single-shard table.
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_2.test_table'::regclass AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;

-- (on coordinator) verify that colocation id is properly set for non-empty tenant schema
SELECT colocation_id = (
    SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_2.test_table'::regclass
)
FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text = 'tenant_2';

-- (on workers) verify that colocation id is properly set for non-empty tenant schema
SELECT result FROM run_command_on_workers($$
    SELECT colocation_id = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_2.test_table'::regclass
    )
    FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text = 'tenant_2';
$$);

-- create a tenant table for tenant_1 after add_node
CREATE TABLE tenant_1.test_table(a int, b text);

-- (on coordinator) verify that colocation id is properly set for now-non-empty tenant schema
SELECT colocation_id = (
    SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_1.test_table'::regclass
)
FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text = 'tenant_1';

-- (on workers) verify that colocation id is properly set for now-non-empty tenant schema
SELECT result FROM run_command_on_workers($$
    SELECT colocation_id = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_1.test_table'::regclass
    )
    FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text = 'tenant_1';
$$);

-- verify that tenant_1 and tenant_2 have different colocation ids
SELECT COUNT(DISTINCT(colocation_id))=2 FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text IN ('tenant_1', 'tenant_2');

CREATE SCHEMA tenant_4;
CREATE TABLE tenant_4.tbl_1(a int, b text);
CREATE TABLE tenant_4.tbl_2(a int, b text);

CREATE SCHEMA tenant_5;
CREATE TABLE tenant_5.tbl_1(a int, b text);

CREATE TABLE tenant_4.tbl_3(a int, b text);

CREATE TABLE tenant_5.tbl_2(a int, b text);

SET citus.enable_schema_based_sharding TO OFF;

-- Show that the tables created in tenant schemas are considered to be
-- tenant tables even if the GUC was set to off when creating the table.
CREATE TABLE tenant_5.tbl_3(a int, b text);
SELECT COUNT(*)=1 FROM pg_dist_partition WHERE logicalrelid = 'tenant_5.tbl_3'::regclass;

SET citus.enable_schema_based_sharding TO ON;

-- Verify that tables that belong to tenant_4 and tenant_5 are stored on
-- different worker nodes due to order we followed when creating first tenant
-- tables in each of them.
SELECT COUNT(DISTINCT(nodename, nodeport))=2 FROM citus_shards
WHERE table_name IN ('tenant_4.tbl_1'::regclass, 'tenant_5.tbl_1'::regclass);

-- show that all the tables in tenant_4 are colocated with each other.
SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition
WHERE logicalrelid::regclass::text LIKE 'tenant_4.%';

-- verify the same for tenant_5 too
SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition
WHERE logicalrelid::regclass::text LIKE 'tenant_5.%';

SET client_min_messages TO WARNING;
DROP SCHEMA tenant_4 CASCADE;
SET client_min_messages TO NOTICE;

-- (on coordinator) Verify that dropping a tenant schema deletes the associated
-- pg_dist_tenant_schema entry too.
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text = 'tenant_4';

-- (on workers) Verify that dropping a tenant schema deletes the associated
-- pg_dist_tenant_schema entry too.
SELECT result FROM run_command_on_workers($$
    SELECT COUNT(*)=0 FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text = 'tenant_4';
$$);

-- show that we don't allow colocating a Citus table with a tenant table
CREATE TABLE regular_schema.null_shard_key_1(a int, b text);
SELECT create_distributed_table('regular_schema.null_shard_key_1', null, colocate_with => 'tenant_5.tbl_2');
SELECT create_distributed_table('regular_schema.null_shard_key_1', 'a', colocate_with => 'tenant_5.tbl_2');

CREATE TABLE regular_schema.null_shard_key_table_2(a int, b text);
SELECT create_distributed_table('regular_schema.null_shard_key_table_2', null);

-- Show that we don't chose to colocate regular single-shard tables with
-- tenant tables by default.
SELECT * FROM pg_dist_tenant_schema WHERE colocation_id = (
    SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'regular_schema.null_shard_key_table_2'::regclass
);

-- save the colocation id used for tenant_5
SELECT colocation_id AS tenant_5_old_colocation_id FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text = 'tenant_5' \gset

-- drop all the tables that belong to tenant_5 and create a new one
DROP TABLE tenant_5.tbl_1, tenant_5.tbl_2, tenant_5.tbl_3;
CREATE TABLE tenant_5.tbl_4(a int, b text);

-- (on coordinator) verify that tenant_5 is now associated with a new colocation id
SELECT colocation_id != :tenant_5_old_colocation_id FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text = 'tenant_5';

-- (on workers) verify that tenant_5 is now associated with a new colocation id
SELECT format(
    'SELECT result FROM run_command_on_workers($$
        SELECT colocation_id != %s FROM pg_dist_tenant_schema
        WHERE schema_id::regnamespace::text = ''tenant_5'';
    $$);',
:tenant_5_old_colocation_id) AS verify_workers_query \gset

:verify_workers_query

SET client_min_messages TO WARNING;
SET citus.enable_schema_based_sharding TO OFF;

DROP SCHEMA tenant_1, tenant_2 CASCADE;

SET client_min_messages TO NOTICE;

-- (on coordinator) Verify that dropping a tenant schema always deletes
-- the associated pg_dist_tenant_schema entry even if the the schema was
-- dropped while the GUC was set to off.
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema
WHERE schema_id::regnamespace::text IN ('tenant_1', 'tenant_2');

-- (on workers) Verify that dropping a tenant schema always deletes
-- the associated pg_dist_tenant_schema entry even if the the schema was
-- dropped while the GUC was set to off.
SELECT result FROM run_command_on_workers($$
    SELECT COUNT(*)=0 FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text IN ('tenant_1', 'tenant_2');
$$);

SET citus.enable_schema_based_sharding TO ON;
SET client_min_messages TO NOTICE;

-- show that all schema_id values are unique and non-null in pg_dist_tenant_schema
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id IS NULL;
SELECT (SELECT COUNT(*) FROM pg_dist_tenant_schema) =
       (SELECT COUNT(DISTINCT(schema_id)) FROM pg_dist_tenant_schema);

-- show that all colocation_id values are unique in pg_dist_tenant_schema
SELECT (SELECT COUNT(*) FROM pg_dist_tenant_schema) =
       (SELECT COUNT(DISTINCT(colocation_id)) FROM pg_dist_tenant_schema) +
       (SELECT COUNT(*) FROM pg_dist_tenant_schema WHERE colocation_id IS NULL);

CREATE TABLE public.cannot_be_a_tenant_table(a int, b text);

-- show that we don't consider public schema as a tenant schema
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id::regnamespace::text = 'public';

DROP TABLE public.cannot_be_a_tenant_table;

BEGIN;
    ALTER SCHEMA public RENAME TO public_renamed;
    CREATE SCHEMA public;

    -- Show that we don't consider public schema as a tenant schema,
    -- even if it's recreated.
    SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id::regnamespace::text = 'public';
ROLLBACK;

CREATE TEMPORARY TABLE temp_table(a int, b text);

-- show that we don't consider temporary schemas as tenant schemas
SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id::regnamespace::text = '%pg_temp%';

DROP TABLE temp_table;

-- test the UDFs that we use to sync pg_dist_tenant_schema to workers, all should fail
SELECT pg_catalog.citus_internal_insert_tenant_schema(1, 1);
SELECT pg_catalog.citus_internal_set_tenant_schema_colocation_id(1, 1);
SELECT pg_catalog.citus_internal_delete_tenant_schema(1);

-- test creating a tenant schema and a tenant table for it in the same transaction
BEGIN;
    CREATE SCHEMA tenant_7;
    CREATE TABLE tenant_7.tbl_1(a int, b text);
    CREATE TABLE tenant_7.tbl_2(a int, b text);

    SELECT colocation_id = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_7.tbl_1'::regclass
    )
    FROM pg_dist_tenant_schema
    WHERE schema_id::regnamespace::text = 'tenant_7';

    -- make sure that both tables created in tenant_7 are colocated
    SELECT COUNT(DISTINCT(colocationid)) = 1 FROM pg_dist_partition
    WHERE logicalrelid IN ('tenant_7.tbl_1'::regclass, 'tenant_7.tbl_2'::regclass);
COMMIT;

-- Test creating a tenant schema and a tenant table for it in the same transaction
-- but this time rollback the transaction.
BEGIN;
    CREATE SCHEMA tenant_8;
    CREATE TABLE tenant_8.tbl_1(a int, b text);
    CREATE TABLE tenant_8.tbl_2(a int, b text);
ROLLBACK;

SELECT COUNT(*)=0 FROM pg_dist_tenant_schema WHERE schema_id::regnamespace::text = 'tenant_8';
SELECT COUNT(*)=0 FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant_8.%';

-- Verify that citus.enable_schema_based_sharding and citus.use_citus_managed_tables
-- GUC don't interfere with each other when creating a table in tenant schema.
--
-- In utility hook, we check whether the CREATE TABLE command is issued on a tenant
-- schema before checking whether citus.use_citus_managed_tables is set to ON to
-- avoid converting the table into a Citus managed table unnecessarily.
--
-- If the CREATE TABLE command is issued on a tenant schema, we skip the check
-- for citus.use_citus_managed_tables.
SET citus.use_citus_managed_tables TO ON;
CREATE TABLE tenant_7.tbl_3(a int, b text, PRIMARY KEY(a));
RESET citus.use_citus_managed_tables;

-- Verify that we don't unnecessarily convert a table into a Citus managed
-- table when creating it with a pre-defined foreign key to a reference table.
CREATE TABLE reference_table(a int PRIMARY KEY);
SELECT create_reference_table('reference_table');

-- Notice that tenant_7.tbl_4 have foreign keys both to tenant_7.tbl_3 and
-- to reference_table.
CREATE TABLE tenant_7.tbl_4(a int REFERENCES reference_table, FOREIGN KEY(a) REFERENCES tenant_7.tbl_3(a));

SELECT COUNT(*)=2 FROM pg_dist_partition
WHERE logicalrelid IN ('tenant_7.tbl_3'::regclass, 'tenant_7.tbl_4'::regclass) AND
      partmethod = 'n' AND repmodel = 's' AND
      colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_7.tbl_1'::regclass);

CREATE TABLE local_table(a int PRIMARY KEY);

-- fails because tenant tables cannot have foreign keys to local tables
CREATE TABLE tenant_7.tbl_5(a int REFERENCES local_table(a));

-- Fails because tenant tables cannot have foreign keys to tenant tables
-- that belong to different tenant schemas.
CREATE TABLE tenant_5.tbl_5(a int, b text, FOREIGN KEY(a) REFERENCES tenant_7.tbl_3(a));

\c - - - :worker_1_port

-- test creating a tenant table from workers
CREATE TABLE tenant_3.tbl_1(a int, b text);

-- test creating a tenant schema from workers
SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA worker_tenant_schema;
SET citus.enable_schema_based_sharding TO OFF;

-- Enable the GUC on workers to make sure that the CREATE SCHEMA/ TABLE
-- commands that we send to workers don't recursively try creating a
-- tenant schema / table.
ALTER SYSTEM SET citus.enable_schema_based_sharding TO ON;
SELECT pg_reload_conf();

\c - - - :worker_2_port

ALTER SYSTEM SET citus.enable_schema_based_sharding TO ON;
SELECT pg_reload_conf();

\c - - - :master_port

SET search_path TO regular_schema;
SET citus.next_shard_id TO 1940000;
SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

CREATE TABLE tenant_3.tbl_1(a int, b text);

SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA tenant_6;
CREATE TABLE tenant_6.tbl_1(a int, b text);

-- verify pg_dist_partition entries for tenant_3.tbl_1 and tenant_6.tbl_1
SELECT COUNT(*)=2 FROM pg_dist_partition
WHERE logicalrelid IN ('tenant_3.tbl_1'::regclass, 'tenant_6.tbl_1'::regclass) AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;

\c - - - :worker_1_port

ALTER SYSTEM RESET citus.enable_schema_based_sharding;
SELECT pg_reload_conf();

\c - - - :worker_2_port

ALTER SYSTEM RESET citus.enable_schema_based_sharding;
SELECT pg_reload_conf();

\c - - - :master_port

SET client_min_messages TO WARNING;
DROP SCHEMA regular_schema, tenant_3, tenant_5, tenant_7, tenant_6 CASCADE;

SELECT citus_remove_node('localhost', :master_port);
