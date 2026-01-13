-- This is heavily based on schema_based_sharding.sql test file.
-- Only differences are;
--   - we don't check some of the the functionality tested there (e.g., testing of some of the internal UDFs)
--   - we test schema-based sharding features (e.g., DDLs, query etc.) using the same SQL from workers this time
--   - when we verify the things, we always make sure to do that on all nodes to ensure that we consistently sync
--     metadata changes when a command is issued from the workers too

SET client_min_messages TO WARNING;

SELECT 1 FROM citus_add_node('localhost', :master_port, groupid => 0);

SET client_min_messages TO NOTICE;

SELECT 1 FROM citus_remove_node('localhost', :worker_2_port);

\c - - - :worker_1_port

-- When creating a tenant table from workers, we always fetch the next shard id
-- and placement id from the coordinator because we never sync those sequences to
-- workers. For this reason, along this test file, we always set the next shard id
-- on the coordinator when needed, rather than setting it on the current worker node.
-- At the end of the test file, we reset it back fwiw.
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050000;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1); -- make sure that the GUC change is applied

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

SET citus.enable_schema_based_sharding TO ON;

-- empty tenant
CREATE SCHEMA "tenant\'_1";
CREATE SCHEMA IF NOT EXISTS "tenant\'_1";

-- non-empty tenant
CREATE SCHEMA "tenant\'_2";
CREATE TABLE "tenant\'_2".test_table(a int, b text);

-- empty tenant
CREATE SCHEMA "tenant\'_3";
CREATE TABLE "tenant\'_3".test_table(a int, b text);
DROP TABLE "tenant\'_3".test_table;

\c - - - :master_port

-- add a node after creating tenant schemas
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050100;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

CREATE SCHEMA regular_schema;
SET search_path TO regular_schema;

-- Verify that citus_internal.unregister_tenant_schema_globally can only
-- be called on schemas that are dropped already.
SELECT citus_internal.unregister_tenant_schema_globally('regular_schema'::regnamespace, 'regular_schema');

-- show that regular_schema doesn't show up in pg_dist_schema
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE schemaid::regnamespace::text = 'regular_schema';

CREATE TABLE regular_schema.citus_local_tbl(id int);
SELECT citus_add_local_table_to_metadata('regular_schema.citus_local_tbl');

CREATE TABLE regular_schema.hash_dist_tbl(id int);
SELECT create_distributed_table('regular_schema.hash_dist_tbl', 'id');

CREATE TABLE regular_schema.ref_tbl(id int  PRIMARY KEY);
SELECT create_reference_table('regular_schema.ref_tbl');

CREATE TABLE regular_schema.ref_tbl_1(id int  PRIMARY KEY);
SELECT create_reference_table('regular_schema.ref_tbl_1');

CREATE TABLE regular_schema.pg_local_tbl3(id int REFERENCES regular_schema.ref_tbl_1(id));

CREATE TABLE regular_schema.citus_local_partitioned_table(a int, b text) PARTITION BY RANGE (a);
SELECT citus_add_local_table_to_metadata('regular_schema.citus_local_partitioned_table');

CREATE TABLE regular_schema.dist_partitioned_table(a int, b text) PARTITION BY RANGE (a);
SELECT create_distributed_table('regular_schema.dist_partitioned_table', 'a');

CREATE TABLE regular_schema.parent_attach_test_citus_local(a int, b text) PARTITION BY RANGE (a);
SELECT citus_add_local_table_to_metadata('regular_schema.parent_attach_test_citus_local');

CREATE TABLE regular_schema.parent_attach_test_dist(a int, b text) PARTITION BY RANGE (a);
SELECT create_distributed_table('regular_schema.parent_attach_test_dist', 'a');

CREATE TABLE regular_schema.child_attach_test_citus_local(a int, b text);
SELECT citus_add_local_table_to_metadata('regular_schema.child_attach_test_citus_local');

CREATE TABLE regular_schema.child_attach_test_dist(a int, b text);
SELECT create_distributed_table('regular_schema.child_attach_test_dist', 'a');

CREATE TABLE regular_schema.citus_local(a int, b text);
SELECT citus_add_local_table_to_metadata('regular_schema.citus_local');

CREATE TABLE regular_schema.dist(a int, b text);
SELECT create_distributed_table('regular_schema.dist', 'a');

CREATE TYPE regular_schema.employee_type AS (name text, salary numeric);

CREATE TABLE regular_schema.reference_table(a int PRIMARY KEY);
SELECT create_reference_table('regular_schema.reference_table');

CREATE FUNCTION regular_schema.increment_one()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE search_path_test SET a = a + 1;
END;
$$;

CREATE FUNCTION regular_schema.decrement_one()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE search_path_test SET a = a - 1;
END;
$$;

\c - - - :worker_1_port

SET citus.enable_schema_based_sharding TO ON;
SET client_min_messages TO NOTICE;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050300;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

ALTER SCHEMA "tenant\'_1" RENAME TO tenant_1;
ALTER SCHEMA "tenant\'_2" RENAME TO tenant_2;
ALTER SCHEMA "tenant\'_3" RENAME TO tenant_3;

-- verify we cannot set tenant table's schema to regular schema from workers
CREATE TABLE tenant_2.test_table2(id int);
ALTER TABLE tenant_2.test_table2 SET SCHEMA regular_schema;

-- verify we can set regular table's schema to distributed schema
CREATE TABLE regular_schema.test_table3(id int);
ALTER TABLE regular_schema.test_table3 SET SCHEMA tenant_2;
-- verify that tenant_2.test_table3 is recorded in pg_dist_partition as a single-shard table.
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_2.test_table3'::regclass AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;
$$);
-- verify that regular_schema.test_table3 does not exist
SELECT * FROM regular_schema.test_table3;

-- verify we cannot set tenant table's schema to another distributed schema from workers
CREATE TABLE tenant_2.test_table4(id int);
ALTER TABLE tenant_2.test_table4 SET SCHEMA tenant_3;

-- verify that we can put a local table in regular schema into distributed schema
CREATE TABLE regular_schema.pg_local_tbl(id int);
ALTER TABLE regular_schema.pg_local_tbl SET SCHEMA tenant_2;

-- verify that we can put a Citus local table in regular schema into distributed schema
ALTER TABLE regular_schema.citus_local_tbl SET SCHEMA tenant_2;

-- verify that we do not allow a hash distributed table in regular schema into distributed schema
ALTER TABLE regular_schema.hash_dist_tbl SET SCHEMA tenant_2;

-- verify that we do not allow a reference table in regular schema into distributed schema
ALTER TABLE regular_schema.ref_tbl SET SCHEMA tenant_2;

-- verify that we cannot put a table in tenant schema into regular schema
CREATE TABLE tenant_2.tenant_tbl(id int);
ALTER TABLE tenant_2.tenant_tbl SET SCHEMA regular_schema;

-- verify that we cannot put a table in tenant schema into another tenant schema
CREATE TABLE tenant_2.tenant_tbl2(id int);
ALTER TABLE tenant_2.tenant_tbl2 SET SCHEMA tenant_3;

-- verify that we do not allow a local table in regular schema into distributed schema if it has foreign key to a non-reference table in another schema
CREATE TABLE regular_schema.pg_local_tbl1(id int PRIMARY KEY);
CREATE TABLE regular_schema.pg_local_tbl2(id int REFERENCES regular_schema.pg_local_tbl1(id));
ALTER TABLE regular_schema.pg_local_tbl2 SET SCHEMA tenant_2;

-- verify that we allow a local table in regular schema into distributed schema if it has foreign key to a reference table in another schema
ALTER TABLE regular_schema.pg_local_tbl3 SET SCHEMA tenant_2;

-- verify that we do not allow a table in tenant schema into regular schema if it has foreign key to/from another table in the same schema
DROP TABLE tenant_2.tenant_tbl2;

CREATE TABLE tenant_2.tenant_tbl1(id int PRIMARY KEY);
CREATE TABLE tenant_2.tenant_tbl2(id int REFERENCES tenant_2.tenant_tbl1(id));
ALTER TABLE tenant_2.tenant_tbl1 SET SCHEMA regular_schema;
ALTER TABLE tenant_2.tenant_tbl2 SET SCHEMA regular_schema;

-- verify that we do not allow a table in distributed schema into another distributed schema if it has foreign key to/from another table in the same schema
CREATE TABLE tenant_2.tenant_tbl3(id int PRIMARY KEY);
CREATE TABLE tenant_2.tenant_tbl4(id int REFERENCES tenant_2.tenant_tbl3(id));
ALTER TABLE tenant_2.tenant_tbl3 SET SCHEMA tenant_3;
ALTER TABLE tenant_2.tenant_tbl4 SET SCHEMA tenant_3;

-- alter set non-existent schema
ALTER TABLE tenant_2.test_table SET SCHEMA ghost_schema;
ALTER TABLE IF EXISTS tenant_2.test_table SET SCHEMA ghost_schema;
-- alter set non-existent table
ALTER TABLE tenant_2.ghost_table SET SCHEMA ghost_schema;
ALTER TABLE IF EXISTS tenant_2.ghost_table SET SCHEMA ghost_schema;

-- verify that colocation id is set for empty tenants too
SELECT result FROM run_command_on_all_nodes($$
    SELECT array_agg(colocationid > 0) FROM pg_dist_schema
    WHERE schemaid::regnamespace::text IN ('tenant_1', 'tenant_3');
$$);

-- Verify that tenant_2.test_table is recorded in pg_dist_partition as a
-- single-shard table.
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_2.test_table'::regclass AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;
$$);

-- verify that colocation id is properly set for non-empty tenant schema
SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_2.test_table'::regclass
    )
    FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_2';
$$);

-- create a tenant table for tenant_1 after add_node
CREATE TABLE tenant_1.test_table(a int, b text);

-- verify that colocation id is properly set for now-non-empty tenant schema
SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_1.test_table'::regclass
    )
    FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_1';
$$);

-- verify that tenant_1 and tenant_2 have different colocation ids
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(DISTINCT(colocationid))=2 FROM pg_dist_schema
WHERE schemaid::regnamespace::text IN ('tenant_1', 'tenant_2');
$$);

-- verify that we don't allow creating tenant tables via CREATE SCHEMA command
CREATE SCHEMA schema_using_schema_elements CREATE TABLE test_table(a int, b text);

CREATE SCHEMA tenant_4;
CREATE TABLE tenant_4.tbl_1(a int, b text);
CREATE TABLE tenant_4.tbl_2(a int, b text);

-- verify that we don't allow creating a foreign table in a tenant schema, with a nice error message
CREATE FOREIGN TABLE tenant_4.foreign_table (
  id bigint not null,
  full_name text not null default ''
) SERVER fake_fdw_server OPTIONS (encoding 'utf-8', compression 'true', table_name 'foreign_table');

-- verify that we don't allow creating a foreign table in a tenant schema
CREATE TEMPORARY TABLE tenant_4.temp_table (a int, b text);

CREATE TABLE tenant_4.partitioned_table(a int, b text, PRIMARY KEY (a)) PARTITION BY RANGE (a);
CREATE TABLE tenant_4.partitioned_table_child_1 PARTITION OF tenant_4.partitioned_table FOR VALUES FROM (1) TO (2);

CREATE TABLE tenant_4.another_partitioned_table(a int, b text, FOREIGN KEY (a) REFERENCES tenant_4.partitioned_table(a)) PARTITION BY RANGE (a);
CREATE TABLE tenant_4.another_partitioned_table_child PARTITION OF tenant_4.another_partitioned_table FOR VALUES FROM (1) TO (2);

-- verify that we allow creating partitioned tables in a tenant schema
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_4.partitioned_table_child_1'::regclass AND
       partmethod = 'n' AND repmodel = 's' AND colocationid = (
        SELECT colocationid FROM pg_dist_partition
        WHERE logicalrelid = 'tenant_4.partitioned_table'::regclass);
$$);

SELECT result FROM run_command_on_all_nodes($$
SELECT EXISTS(
    SELECT 1
    FROM pg_inherits
    WHERE inhrelid = 'tenant_4.partitioned_table_child_1'::regclass AND
          inhparent = 'tenant_4.partitioned_table'::regclass
) AS is_partition;
$$);

SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_4.another_partitioned_table_child'::regclass AND
       partmethod = 'n' AND repmodel = 's' AND colocationid = (
        SELECT colocationid FROM pg_dist_partition
        WHERE logicalrelid = 'tenant_4.another_partitioned_table'::regclass);
$$);

SELECT result FROM run_command_on_all_nodes($$
SELECT EXISTS(
    SELECT 1
    FROM pg_inherits
    WHERE inhrelid = 'tenant_4.another_partitioned_table_child'::regclass AND
          inhparent = 'tenant_4.another_partitioned_table'::regclass
) AS is_partition;
$$);

-- verify the foreign key between parents
SELECT result FROM run_command_on_all_nodes($$
SELECT EXISTS(
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'tenant_4.another_partitioned_table'::regclass AND
          confrelid = 'tenant_4.partitioned_table'::regclass AND
          contype = 'f'
) AS foreign_key_exists;
$$);

-- We want to hide the error message context because the node reporting the foreign key
-- violation might change from one run to another.
\set VERBOSITY terse

INSERT INTO tenant_4.another_partitioned_table VALUES (1, 'a');

\set VERBOSITY default

INSERT INTO tenant_4.partitioned_table VALUES (1, 'a');
INSERT INTO tenant_4.another_partitioned_table VALUES (1, 'a');

CREATE SCHEMA tenant_5;
CREATE TABLE tenant_5.tbl_1(a int, b text);

CREATE TABLE tenant_5.partitioned_table(a int, b text) PARTITION BY RANGE (a);

-- verify that we don't allow creating a partition table that is child of a partitioned table in a different tenant schema
CREATE TABLE tenant_4.partitioned_table_child_2 PARTITION OF tenant_5.partitioned_table FOR VALUES FROM (1) TO (2);

-- verify that we don't allow creating a local partition table that is child of a tenant partitioned table
CREATE TABLE regular_schema.local_child_table PARTITION OF tenant_5.partitioned_table FOR VALUES FROM (1) TO (2);

SET citus.use_citus_managed_tables TO ON;
CREATE TABLE regular_schema.local_child_table PARTITION OF tenant_5.partitioned_table FOR VALUES FROM (1) TO (2);
RESET citus.use_citus_managed_tables;

CREATE TABLE regular_schema.local_partitioned_table(a int, b text) PARTITION BY RANGE (a);

-- verify that we don't allow creating a partition table that is child of a non-tenant partitioned table
CREATE TABLE tenant_4.partitioned_table_child_2 PARTITION OF regular_schema.local_partitioned_table FOR VALUES FROM (1) TO (2);
CREATE TABLE tenant_4.partitioned_table_child_2 PARTITION OF regular_schema.citus_local_partitioned_table FOR VALUES FROM (1) TO (2);
CREATE TABLE tenant_4.partitioned_table_child_2 PARTITION OF regular_schema.dist_partitioned_table FOR VALUES FROM (1) TO (2);

CREATE TABLE tenant_4.parent_attach_test(a int, b text) PARTITION BY RANGE (a);
CREATE TABLE tenant_4.child_attach_test(a int, b text);

CREATE TABLE tenant_5.parent_attach_test(a int, b text) PARTITION BY RANGE (a);
CREATE TABLE tenant_5.child_attach_test(a int, b text);

CREATE TABLE regular_schema.parent_attach_test_local(a int, b text) PARTITION BY RANGE (a);

CREATE TABLE regular_schema.child_attach_test_local(a int, b text);

-- verify that we don't allow attaching a tenant table into a tenant partitioned table, if they are not in the same schema
ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION tenant_5.child_attach_test FOR VALUES FROM (1) TO (2);

-- verify that we don't allow attaching a non-tenant table into a tenant partitioned table
ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION regular_schema.child_attach_test_local FOR VALUES FROM (1) TO (2);
ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION regular_schema.child_attach_test_citus_local FOR VALUES FROM (1) TO (2);
ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION regular_schema.child_attach_test_dist FOR VALUES FROM (1) TO (2);

-- verify that we don't allow attaching a tenant table into a non-tenant partitioned table
ALTER TABLE regular_schema.parent_attach_test_local ATTACH PARTITION tenant_4.child_attach_test FOR VALUES FROM (1) TO (2);
ALTER TABLE regular_schema.parent_attach_test_citus_local ATTACH PARTITION tenant_4.child_attach_test FOR VALUES FROM (1) TO (2);
ALTER TABLE regular_schema.parent_attach_test_dist ATTACH PARTITION tenant_4.child_attach_test FOR VALUES FROM (1) TO (2);

ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION tenant_4.child_attach_test FOR VALUES FROM (1) TO (2);

-- verify that we don't allow multi-level partitioning on tenant tables
CREATE TABLE tenant_4.multi_level_test(a int, b text) PARTITION BY RANGE (a);
ALTER TABLE tenant_4.parent_attach_test ATTACH PARTITION tenant_4.multi_level_test FOR VALUES FROM (1) TO (2);

-- verify that we allow attaching a tenant table into a tenant partitioned table, if they are in the same schema
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition
WHERE logicalrelid = 'tenant_4.parent_attach_test'::regclass AND
       partmethod = 'n' AND repmodel = 's' AND colocationid = (
        SELECT colocationid FROM pg_dist_partition
        WHERE logicalrelid = 'tenant_4.child_attach_test'::regclass);
$$);

SELECT result FROM run_command_on_all_nodes($$
SELECT EXISTS(
    SELECT 1
    FROM pg_inherits
    WHERE inhrelid = 'tenant_4.child_attach_test'::regclass AND
          inhparent = 'tenant_4.parent_attach_test'::regclass
) AS is_partition;
$$);

-- errors out because shard replication factor > 1
SET citus.shard_replication_factor TO 2;
CREATE TABLE tenant_4.tbl_3 AS SELECT 1 AS a, 'text' as b;
SET citus.shard_replication_factor TO 1;
-- verify that we allow creating tenant tables by using CREATE TABLE AS / SELECT INTO commands
CREATE TABLE tenant_4.tbl_3 AS SELECT 1 AS a, 'text' as b;
CREATE TEMP TABLE IF NOT EXISTS tenant_4.tbl_4 AS SELECT 1 as a, 'text' as b;
CREATE UNLOGGED TABLE IF NOT EXISTS tenant_4.tbl_4 AS SELECT 1 as a, 'text' as b WITH NO DATA;
-- the same command, no changes because of IF NOT EXISTS
CREATE UNLOGGED TABLE IF NOT EXISTS tenant_4.tbl_4 AS SELECT 1 as a, 'text' as b WITH NO DATA;
SELECT 1 as a, 'text' as b INTO tenant_4.tbl_5;

-- verify we can query the newly created tenant tables
SELECT * FROM tenant_4.tbl_3;
SELECT COUNT(*) FROM tenant_4.tbl_5;

-- verify that we don't allow creating tenant tables by using CREATE TABLE OF commands
CREATE TABLE tenant_4.employees OF regular_schema.employee_type (
    PRIMARY KEY (name),
    salary WITH OPTIONS DEFAULT 1000
);

-- verify that we act accordingly when if not exists is used
CREATE TABLE IF NOT EXISTS tenant_4.tbl_6(a int, b text);
CREATE TABLE IF NOT EXISTS tenant_4.tbl_6(a int, b text);

SELECT result FROM run_command_on_all_nodes($$
SELECT jsonb_agg(
         jsonb_build_object(
           'logicalrelid', logicalrelid,
           'partmethod',  partmethod
         )
         ORDER BY logicalrelid::text
       )
    FROM pg_dist_partition
    WHERE logicalrelid::text LIKE 'tenant_4.tbl%'
$$);

CREATE TABLE regular_schema.local(a int, b text);

-- verify that we can create a table LIKE another table
CREATE TABLE tenant_5.test_table_like_1(LIKE tenant_5.tbl_1); -- using a table from the same schema
CREATE TABLE tenant_5.test_table_like_2(LIKE tenant_4.tbl_1); -- using a table from another schema
CREATE TABLE tenant_5.test_table_like_3(LIKE regular_schema.local); -- using a local table
CREATE TABLE tenant_5.test_table_like_4(LIKE regular_schema.citus_local); -- using a citus local table
CREATE TABLE tenant_5.test_table_like_5(LIKE regular_schema.dist); -- using a distributed table

-- verify that all of them are converted to tenant tables
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*) = 5
FROM pg_dist_partition
WHERE logicalrelid::text LIKE 'tenant_5.test_table_like_%' AND
      partmethod = 'n' AND repmodel = 's' AND colocationid = (
        SELECT colocationid FROM pg_dist_schema
        WHERE schemaid::regnamespace::text = 'tenant_5'
        );
$$);

CREATE TABLE regular_schema.local_table_using_like(LIKE tenant_5.tbl_1);

-- verify that regular_schema.local_table_using_like is not a tenant table
SELECT COUNT(*) = 0 FROM pg_dist_partition
WHERE logicalrelid = 'regular_schema.local_table_using_like'::regclass;

-- verify that INHERITS syntax is not supported when creating a tenant table
CREATE TABLE tenant_5.test_table_inherits_1(x int) INHERITS (tenant_5.tbl_1); -- using a table from the same schema
CREATE TABLE tenant_5.test_table_inherits_2(x int) INHERITS (tenant_4.tbl_1); -- using a table from another schema
CREATE TABLE tenant_5.test_table_inherits_3(x int) INHERITS (regular_schema.local); -- using a local table
CREATE TABLE tenant_5.test_table_inherits_4(x int) INHERITS (regular_schema.citus_local); -- using a citus local table
CREATE TABLE tenant_5.test_table_inherits_5(x int) INHERITS (regular_schema.dist); -- using a distributed table

-- verify that INHERITS syntax is not supported when creating a local table based on a tenant table
CREATE TABLE regular_schema.local_table_using_inherits(x int) INHERITS (tenant_5.tbl_1);

CREATE TABLE tenant_5.tbl_2(a int, b text);

CREATE SCHEMA "CiTuS.TeeN_108";
ALTER SCHEMA "CiTuS.TeeN_108" RENAME TO citus_teen_proper;

SELECT schemaid AS citus_teen_schemaid FROM pg_dist_schema WHERE schemaid::regnamespace::text = 'citus_teen_proper' \gset
SELECT colocationid AS citus_teen_colocationid FROM pg_dist_schema WHERE schemaid::regnamespace::text = 'citus_teen_proper' \gset

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO citus_teen_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'citus_teen_proper'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO citus_teen_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'citus_teen_proper'
$$);

-- verify that colocation id is set for the tenant with a weird name too
SELECT :citus_teen_colocationid > 0;

-- verify that the same colocation id is used on other nodes too
SELECT format(
    'SELECT result FROM run_command_on_all_nodes($$
        SELECT COUNT(*)=1 FROM pg_dist_schema
        WHERE schemaid::regnamespace::text = ''citus_teen_proper'' AND
              colocationid = %s;
    $$);',
:citus_teen_colocationid) AS verify_all_nodes_query \gset

:verify_all_nodes_query

ALTER SCHEMA citus_teen_proper RENAME TO "CiTuS.TeeN_108";

SET citus.enable_schema_based_sharding TO OFF;

-- Show that the tables created in tenant schemas are considered to be
-- tenant tables even if the GUC was set to off when creating the table.
CREATE TABLE tenant_5.tbl_3(a int, b text);
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=1 FROM pg_dist_partition WHERE logicalrelid = 'tenant_5.tbl_3'::regclass;
$$);

SET citus.enable_schema_based_sharding TO ON;

-- Verify that tables that belong to tenant_4 and tenant_5 are stored on
-- different worker nodes due to order we followed when creating first tenant
-- tables in each of them.
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(DISTINCT(nodename, nodeport))=2 FROM citus_shards
WHERE table_name IN ('tenant_4.tbl_1'::regclass, 'tenant_5.tbl_1'::regclass);
$$);

-- show that all the tables in tenant_4 are colocated with each other.
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition
WHERE logicalrelid::regclass::text LIKE 'tenant_4.%';
$$);

-- verify the same for tenant_5 too
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(DISTINCT(colocationid))=1 FROM pg_dist_partition
WHERE logicalrelid::regclass::text LIKE 'tenant_5.%';
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_4_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_4'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_4_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_4'
$$);

SET client_min_messages TO WARNING;

-- Rename it to a name that contains a single quote to verify that we properly
-- escape its name when sending the command to delete the pg_dist_schema
-- entry on workers.
ALTER SCHEMA tenant_4 RENAME TO "tenant\'_4";

DROP SCHEMA "tenant\'_4", "CiTuS.TeeN_108" CASCADE;

SET client_min_messages TO NOTICE;

-- Verify that dropping a tenant schema deletes the associated
-- pg_dist_schema entry and pg_dist_colocation too.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid = (SELECT schemaid FROM tenant_4_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid = (SELECT schemaid FROM citus_teen_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE colocationid = (SELECT colocationid FROM tenant_4_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE colocationid = (SELECT colocationid FROM citus_teen_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    DROP TABLE tenant_4_schemaid, citus_teen_schemaid, tenant_4_colocationid, citus_teen_colocationid
$$);

\c - - - :master_port

SET client_min_messages TO NOTICE;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050400;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- show that we don't allow colocating a Citus table with a tenant table
CREATE TABLE regular_schema.null_shard_key_1(a int, b text);
SELECT create_distributed_table('regular_schema.null_shard_key_1', null, colocate_with => 'tenant_5.tbl_2');
SELECT create_distributed_table('regular_schema.null_shard_key_1', 'a', colocate_with => 'tenant_5.tbl_2');

CREATE TABLE regular_schema.null_shard_key_table_2(a int, b text);
SELECT create_distributed_table('regular_schema.null_shard_key_table_2', null);

-- let's switch to a different worker node for the rest of the tests
\c - - - :worker_2_port

SET citus.enable_schema_based_sharding TO ON;
SET client_min_messages TO NOTICE;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050500;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

-- Show that we don't chose to colocate regular single-shard tables with
-- tenant tables by default.
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE colocationid = (
    SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'regular_schema.null_shard_key_table_2'::regclass
);
$$);

-- save the colocation id used for tenant_5
SELECT colocationid AS tenant_5_old_colocationid FROM pg_dist_schema
WHERE schemaid::regnamespace::text = 'tenant_5' \gset

-- drop all the tables that belong to tenant_5 and create a new one
DROP TABLE tenant_5.tbl_1, tenant_5.tbl_2, tenant_5.tbl_3;
CREATE TABLE tenant_5.tbl_4(a int, b text);

-- verify that tenant_5 is still associated with the same colocation id
SELECT format(
    'SELECT result FROM run_command_on_all_nodes($$
        SELECT colocationid = %s FROM pg_dist_schema
        WHERE schemaid::regnamespace::text = ''tenant_5'';
    $$);',
:tenant_5_old_colocationid) AS verify_all_nodes_query \gset

:verify_all_nodes_query

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_1_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_1'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_2_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_2'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_1_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_1'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_2_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_2'
$$);

SET client_min_messages TO WARNING;
SET citus.enable_schema_based_sharding TO OFF;

DROP SCHEMA tenant_1 CASCADE;

CREATE ROLE test_non_super_user;
ALTER ROLE test_non_super_user NOSUPERUSER;

ALTER SCHEMA tenant_2 OWNER TO non_existing_role;
ALTER SCHEMA tenant_2 OWNER TO test_non_super_user;

select result from run_command_on_all_nodes ($$
  SELECT pg_get_userbyid(nspowner) AS schema_owner
  FROM pg_namespace
  WHERE nspname = 'tenant_2'
$$);

\c - - - :master_port

SET client_min_messages TO WARNING;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050600;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

DROP OWNED BY test_non_super_user CASCADE;

\c - - - :worker_2_port

SET citus.enable_schema_based_sharding TO ON;
SET client_min_messages TO NOTICE;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2050700;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;

DROP ROLE test_non_super_user;

-- Verify that dropping a tenant schema always deletes
-- the associated pg_dist_schema entry even if the the schema was
-- dropped while the GUC was set to off.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid IN (SELECT schemaid FROM tenant_1_schemaid UNION SELECT schemaid FROM tenant_2_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE colocationid IN (SELECT colocationid FROM tenant_1_colocationid UNION SELECT colocationid FROM tenant_2_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    DROP TABLE tenant_1_schemaid, tenant_2_schemaid, tenant_1_colocationid, tenant_2_colocationid
$$);

SET citus.enable_schema_based_sharding TO ON;
SET client_min_messages TO NOTICE;

-- show that all schemaid values are unique and non-null in pg_dist_schema
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE schemaid IS NULL;
SELECT (SELECT COUNT(*) FROM pg_dist_schema) =
       (SELECT COUNT(DISTINCT(schemaid)) FROM pg_dist_schema);
$$);

-- show that all colocationid values are unique and non-null in pg_dist_schema
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE colocationid IS NULL;
SELECT (SELECT COUNT(*) FROM pg_dist_schema) =
       (SELECT COUNT(DISTINCT(colocationid)) FROM pg_dist_schema);
$$);

CREATE TABLE public.cannot_be_a_tenant_table(a int, b text);

-- show that we don't consider public schema as a tenant schema
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE schemaid::regnamespace::text = 'public';
$$);

DROP TABLE public.cannot_be_a_tenant_table;

CREATE TEMPORARY TABLE temp_table(a int, b text);

-- show that we don't consider temporary schemas as tenant schemas
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE schemaid::regnamespace::text = '%pg_temp%';
$$);

DROP TABLE temp_table;

-- test creating a tenant schema and a tenant table for it in the same transaction
BEGIN;
    CREATE SCHEMA tenant_7;
    CREATE TABLE tenant_7.tbl_1(a int, b text);
    CREATE TABLE tenant_7.tbl_2(a int, b text);

    SELECT colocationid = (
        SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_7.tbl_1'::regclass
    )
    FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_7';

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

SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_schema WHERE schemaid::regnamespace::text = 'tenant_8';
$$);

SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=0 FROM pg_dist_partition WHERE logicalrelid::text LIKE 'tenant_8.%';
$$);

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

-- Notice that tenant_7.tbl_4 have foreign keys both to tenant_7.tbl_3 and
-- to reference_table.
CREATE TABLE tenant_7.tbl_4(a int REFERENCES regular_schema.reference_table, FOREIGN KEY(a) REFERENCES tenant_7.tbl_3(a) ON DELETE CASCADE);

INSERT INTO tenant_7.tbl_3 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO regular_schema.reference_table VALUES (1), (2), (3);
INSERT INTO tenant_7.tbl_4 VALUES (1), (2), (3);

DELETE FROM tenant_7.tbl_3 WHERE a < 3;
SELECT * FROM tenant_7.tbl_4 ORDER BY a;

SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=2 FROM pg_dist_partition
WHERE logicalrelid IN ('tenant_7.tbl_3'::regclass, 'tenant_7.tbl_4'::regclass) AND
      partmethod = 'n' AND repmodel = 's' AND
      colocationid = (SELECT colocationid FROM pg_dist_partition WHERE logicalrelid = 'tenant_7.tbl_1'::regclass);
$$);

CREATE TABLE local_table(a int PRIMARY KEY);

-- fails because tenant tables cannot have foreign keys to local tables
CREATE TABLE tenant_7.tbl_5(a int REFERENCES local_table(a));

-- Fails because tenant tables cannot have foreign keys to tenant tables
-- that belong to different tenant schemas.
CREATE TABLE tenant_5.tbl_5(a int, b text, FOREIGN KEY(a) REFERENCES tenant_7.tbl_3(a));

CREATE SCHEMA tenant_9;

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_9_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_9'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_9_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_9'
$$);

DROP SCHEMA tenant_9;

-- Make sure that dropping an empty tenant schema
-- doesn't leave any dangling entries in pg_dist_schema and
-- pg_dist_colocation.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid = (SELECT schemaid FROM tenant_9_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_colocation
    WHERE colocationid = (SELECT colocationid FROM tenant_9_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    DROP TABLE tenant_9_schemaid, tenant_9_colocationid
$$);

CREATE TABLE tenant_3.search_path_test(a int);
INSERT INTO tenant_3.search_path_test VALUES (1), (10);

CREATE TABLE tenant_5.search_path_test(a int);
INSERT INTO tenant_5.search_path_test VALUES (2);

CREATE TABLE tenant_7.search_path_test(a int);
INSERT INTO tenant_7.search_path_test VALUES (3);

SET search_path TO tenant_5;

PREPARE list_tuples AS SELECT * FROM search_path_test ORDER BY a;

SELECT * FROM search_path_test ORDER BY a;

SET search_path TO tenant_3;
DELETE FROM search_path_test WHERE a = 1;
SELECT * FROM search_path_test ORDER BY a;
SELECT regular_schema.increment_one();
EXECUTE list_tuples;

SET search_path TO tenant_7;
DROP TABLE search_path_test;
SELECT * FROM pg_dist_partition WHERE logicalrelid::text = 'search_path_test';

SET search_path TO tenant_5;
SELECT regular_schema.decrement_one();
EXECUTE list_tuples;

SET search_path TO regular_schema;

CREATE USER test_other_super_user WITH superuser;

\c - test_other_super_user

SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA tenant_9;

\c - postgres

SET search_path TO regular_schema;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2060000;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;
SET citus.enable_schema_based_sharding TO ON;

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_9_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_9'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_9_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_9'
$$);

\c - - - :master_port

SET client_min_messages TO WARNING;
DROP OWNED BY test_other_super_user;

\c - - - :worker_2_port

SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2060100;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;
SET citus.enable_schema_based_sharding TO ON;

-- Make sure that dropping an empty tenant schema
-- (via DROP OWNED BY) doesn't leave any dangling entries in
-- pg_dist_schema and pg_dist_colocation.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid = (SELECT schemaid FROM tenant_9_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_colocation
    WHERE colocationid = (SELECT colocationid FROM tenant_9_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    DROP TABLE tenant_9_schemaid, tenant_9_colocationid
$$);

DROP USER test_other_super_user;

CREATE ROLE test_non_super_user WITH LOGIN;
ALTER ROLE test_non_super_user NOSUPERUSER;

\c - - - :master_port

GRANT CREATE ON DATABASE regression TO test_non_super_user;

GRANT CREATE ON SCHEMA public TO test_non_super_user ;

\c - test_non_super_user

SET search_path TO regular_schema;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2070000;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;
SET citus.enable_schema_based_sharding TO ON;

-- test create / drop tenant schema / table

CREATE SCHEMA tenant_10;
CREATE TABLE tenant_10.tbl_1(a int, b text);
CREATE TABLE tenant_10.tbl_2(a int, b text);

DROP TABLE tenant_10.tbl_2;

CREATE SCHEMA tenant_11;

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_10_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_10'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT schemaid INTO tenant_11_schemaid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_11'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_10_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_10'
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT colocationid INTO tenant_11_colocationid FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'tenant_11'
$$);

-- Verify metadata for tenant schemas that are created via non-super-user.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(DISTINCT(schemaid))=2 FROM pg_dist_schema
    WHERE schemaid IN (SELECT schemaid FROM tenant_10_schemaid UNION SELECT schemaid FROM tenant_11_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(DISTINCT(colocationid))=2 FROM pg_dist_schema
    WHERE colocationid IN (SELECT colocationid FROM tenant_10_colocationid UNION SELECT colocationid FROM tenant_11_colocationid)
$$);

SET client_min_messages TO WARNING;
DROP SCHEMA tenant_10, tenant_11 CASCADE;
SET client_min_messages TO NOTICE;

-- Verify that dropping a tenant schema via non-super-user
-- deletes the associated pg_dist_schema entry.
SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_schema
    WHERE schemaid IN (SELECT schemaid FROM tenant_10_schemaid UNION SELECT schemaid FROM tenant_11_schemaid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=0 FROM pg_dist_colocation
    WHERE colocationid IN (SELECT colocationid FROM tenant_10_colocationid UNION SELECT colocationid FROM tenant_11_colocationid)
$$);

SELECT result FROM run_command_on_all_nodes($$
    DROP TABLE tenant_10_schemaid, tenant_11_schemaid, tenant_10_colocationid, tenant_11_colocationid
$$);

\c - postgres

REVOKE CREATE ON DATABASE regression FROM test_non_super_user;

REVOKE CREATE ON SCHEMA public FROM test_non_super_user;

DROP ROLE test_non_super_user;

-- Enable the GUC on all nodes to make sure that the CREATE SCHEMA/ TABLE
-- commands that we send to workers don't recursively try creating a
-- tenant schema / table.

\c - - - :master_port

ALTER SYSTEM SET citus.enable_schema_based_sharding TO ON;
SELECT pg_reload_conf();

\c - - - :worker_1_port

ALTER SYSTEM SET citus.enable_schema_based_sharding TO ON;
SELECT pg_reload_conf();

\c - - - :worker_2_port

ALTER SYSTEM SET citus.enable_schema_based_sharding TO ON;
SELECT pg_reload_conf();

-- Verify that citus_internal.unregister_tenant_schema_globally can be called
-- from workers too, but it will fail for this case as we didn't yet drop the
-- schema.
SELECT citus_internal.unregister_tenant_schema_globally('tenant_3'::regnamespace, 'tenant_3');

SET search_path TO regular_schema;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2080000;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

CREATE TABLE tenant_3.tbl_1(a int, b text);

SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA tenant_6;
CREATE TABLE tenant_6.tbl_1(a int, b text);

-- verify pg_dist_partition entries for tenant_3.tbl_1 and tenant_6.tbl_1
SELECT result FROM run_command_on_all_nodes($$
SELECT COUNT(*)=2 FROM pg_dist_partition
WHERE logicalrelid IN ('tenant_3.tbl_1'::regclass, 'tenant_6.tbl_1'::regclass) AND
      partmethod = 'n' AND repmodel = 's' AND colocationid > 0;
$$);

\c - - - :master_port

ALTER SYSTEM RESET citus.enable_schema_based_sharding;
SELECT pg_reload_conf();

\c - - - :worker_1_port

ALTER SYSTEM RESET citus.enable_schema_based_sharding;
SELECT pg_reload_conf();

\c - - - :worker_2_port
SET search_path TO regular_schema;

SET citus.enable_schema_based_sharding TO ON;
SET search_path TO regular_schema;
SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2080200;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

SET citus.shard_count TO 32;
SET citus.shard_replication_factor TO 1;
SET client_min_messages TO NOTICE;

CREATE SCHEMA type_sch;
CREATE TABLE type_sch.tbl (a INT);

SELECT result FROM run_command_on_all_nodes($$
SELECT jsonb_agg(
         jsonb_build_object(
           'table_name', table_name,
           'citus_table_type',  citus_table_type
         )
         ORDER BY table_name::text
       )
FROM public.citus_tables WHERE table_name::text LIKE 'type_sch.tbl';
$$);

SELECT format(
    'SELECT result FROM run_command_on_all_nodes($$
     SELECT jsonb_agg(
            jsonb_build_object(
            ''table_name'', table_name,
            ''citus_table_type'',  citus_table_type
            )
            ORDER BY table_name::text
        )
     FROM citus_shards WHERE table_name::text LIKE ''type_sch.tbl'' AND nodeport IN (%s, %s);
     $$);',
:worker_1_port, :worker_2_port) AS verify_all_nodes_query \gset

:verify_all_nodes_query

RESET citus.enable_schema_based_sharding;

-- test citus_schemas
SET citus.enable_schema_based_sharding TO ON;
CREATE USER citus_schema_role SUPERUSER;

SET ROLE citus_schema_role;

SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2080400;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

CREATE SCHEMA citus_sch1;
CREATE TABLE citus_sch1.tbl1(a INT);
CREATE TABLE citus_sch1.tbl2(a INT);

RESET ROLE;

SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2080500;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

CREATE SCHEMA citus_sch2;
CREATE TABLE citus_sch2.tbl1(a INT);
SET citus.enable_schema_based_sharding TO OFF;

INSERT INTO citus_sch1.tbl1 SELECT * FROM generate_series(1, 10000);
INSERT INTO citus_sch1.tbl2 SELECT * FROM generate_series(1, 5000);

INSERT INTO citus_sch2.tbl1 SELECT * FROM generate_series(1, 12000);

SELECT result FROM run_command_on_all_nodes($$


SELECT jsonb_agg(
         jsonb_build_object(
           'schema_name', cs.schema_name,
           'correct_colocation_id', cs.colocation_id = ctc.colocation_id,
           'correct_size', cs.schema_size = ctc.calculated_size,
           'schema_owner', cs.schema_owner
         )
         ORDER BY schema_name::text
       )
FROM public.citus_schemas cs
JOIN
(
    SELECT
        c.relnamespace, ct.colocation_id,
        pg_size_pretty(sum(citus_total_relation_size(ct.table_name))) AS calculated_size
    FROM public.citus_tables ct, pg_class c
    WHERE ct.table_name::oid = c.oid
    GROUP BY 1, 2
) ctc ON cs.schema_name = ctc.relnamespace
WHERE cs.schema_name::text LIKE 'citus\_sch_'
$$);

-- test empty schema and empty tables
SET citus.enable_schema_based_sharding TO ON;
CREATE SCHEMA citus_empty_sch1;

CREATE SCHEMA citus_empty_sch2;
CREATE TABLE citus_empty_sch2.tbl1(a INT);
SET citus.enable_schema_based_sharding TO OFF;

SELECT result FROM run_command_on_all_nodes($$
SELECT jsonb_agg(
         jsonb_build_object(
           'schema_name', schema_name,
           'schema_size', schema_size
         )
         ORDER BY schema_name::text
       )
FROM public.citus_schemas
WHERE schema_name::text LIKE 'citus\_empty\_sch_';
$$);

-- test with non-privileged role
CREATE USER citus_schema_nonpri;
SET ROLE citus_schema_nonpri;

SET client_min_messages TO ERROR;
SELECT result FROM run_command_on_all_nodes($$
SELECT jsonb_agg(
         jsonb_build_object(
           'schema_name', schema_name,
           'colocation_id_visible', colocation_id > 0,
           'schema_size_visible', schema_size IS NOT NULL,
           'schema_owner', schema_owner
         )
         ORDER BY schema_name::text
       )
FROM public.citus_schemas WHERE schema_name::text LIKE 'citus\_sch_';
$$);

RESET client_min_messages;
RESET ROLE;

SELECT 1 FROM run_command_on_coordinator($$ALTER SYSTEM SET citus.next_shard_id TO 2080600;$$);
SELECT 1 FROM run_command_on_coordinator($$SELECT pg_reload_conf();$$);
SELECT pg_sleep(0.1);

-- test we handle create schema with authorization properly for distributed schema
SET citus.enable_schema_based_sharding TO ON;
CREATE ROLE authschema;
CREATE SCHEMA AUTHORIZATION authschema;
SET citus.enable_schema_based_sharding TO OFF;

SELECT result FROM run_command_on_all_nodes($$
    SELECT COUNT(*)=1
     FROM pg_dist_schema
    WHERE schemaid::regnamespace::text = 'authschema';
$$);

-- mat view can be created under tenant schema
SET citus.enable_schema_based_sharding TO ON;
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA sc1;
CREATE TABLE sc1.t1 (a int);
CREATE MATERIALIZED VIEW sc1.v1 AS SELECT * FROM sc1.t1;
SET citus.enable_schema_based_sharding TO OFF;

SELECT result FROM run_command_on_all_nodes($$
SELECT colocationid > 0 FROM pg_dist_schema
WHERE schemaid::regnamespace::text = 'sc1';
$$);

SET client_min_messages TO WARNING;
DROP TABLE public.local_table;

-- show that we don't allow dropping distributed schemas from workers together with
-- regular propagated schemas
DROP SCHEMA tenant_5, regular_schema, tenant_3 CASCADE;

DROP SCHEMA tenant_3, tenant_5, tenant_7, tenant_6, type_sch, citus_sch1, citus_sch2, citus_empty_sch1, citus_empty_sch2, authschema, sc1 CASCADE;

DROP ROLE citus_schema_role, citus_schema_nonpri, authschema;

\c - - - :master_port

SET client_min_messages TO WARNING;
DROP SCHEMA regular_schema CASCADE;

SELECT citus_remove_node('localhost', :master_port);

-- reset it fwiw
ALTER SYSTEM RESET citus.next_shard_id;
SELECT pg_reload_conf();
SELECT pg_sleep(0.1);
