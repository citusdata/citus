/*
Citus Shard Split Test.The test is model similar to 'shard_move_constraints'.
Here is a high level overview of test plan:
 1. Create a table 'sensors' (ShardCount = 2) to be split. Add indexes and statistics on this table.
 2. Create two other tables: 'reference_table' and 'colocated_dist_table', co-located with sensors.
 3. Create Foreign key constraints between the two co-located distributed tables.
 4. Load data into the three tables.
 5. Move one of the shards for 'sensors' to test ShardMove -> Split.
 6. Trigger Split on both shards of 'sensors'. This will also split co-located tables.
 7. Move one of the split shard to test Split -> ShardMove.
 8. Split an already split shard second time on a different schema.
 9. Create a colocated table with no replica identity.
 10. Show we do not allow Split with the shard transfer mode 'auto' if any colocated table has no replica identity.
 11. Drop the colocated table with no replica identity.
 12. Show we allow Split with the shard transfer mode 'auto' if all colocated tables has replica identity.
*/

CREATE SCHEMA "citus_split_test_schema";

-- Disable Deferred drop auto cleanup to avoid flaky tests.
ALTER SYSTEM SET citus.defer_shard_delete_interval TO -1;
SELECT pg_reload_conf();

CREATE ROLE test_shard_split_role WITH LOGIN;
GRANT USAGE, CREATE ON SCHEMA "citus_split_test_schema" TO test_shard_split_role;
SET ROLE test_shard_split_role;
SET search_path TO "citus_split_test_schema";
SET citus.next_shard_id TO 8981000;
SET citus.next_placement_id TO 8610000;
SET citus.shard_count TO 2;
SET citus.shard_replication_factor TO 1;

-- BEGIN: Create table to split, along with other co-located tables. Add indexes, statistics etc.
CREATE TABLE sensors(
    measureid               integer,
    eventdatetime           date,
    measure_data            jsonb,
	meaure_quantity         decimal(15, 2),
    measure_status          char(1),
	measure_comment         varchar(44),
    PRIMARY KEY (measureid, eventdatetime, measure_data));

CREATE INDEX index_on_sensors ON sensors(lower(measureid::text));
ALTER INDEX index_on_sensors ALTER COLUMN 1 SET STATISTICS 1000;
CREATE INDEX hash_index_on_sensors ON sensors USING HASH((measure_data->'IsFailed'));
CREATE INDEX index_with_include_on_sensors ON sensors ((measure_data->'IsFailed')) INCLUDE (measure_data, eventdatetime, measure_status);
CREATE STATISTICS stats_on_sensors (dependencies) ON measureid, eventdatetime FROM sensors;

SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');
-- END: Create table to split, along with other co-located tables. Add indexes, statistics etc.

-- BEGIN: Create co-located distributed and reference tables.
CREATE TABLE reference_table (measureid integer PRIMARY KEY);
SELECT create_reference_table('reference_table');

CREATE TABLE colocated_dist_table (measureid integer PRIMARY KEY, genid integer GENERATED ALWAYS AS ( measureid + 3 ) stored, value varchar(44), col_todrop integer);
CLUSTER colocated_dist_table USING colocated_dist_table_pkey;
SELECT create_distributed_table('colocated_dist_table', 'measureid', colocate_with:='sensors');

CREATE TABLE table_with_index_rep_identity(key int NOT NULL);
CREATE UNIQUE INDEX uqx ON table_with_index_rep_identity(key);
ALTER TABLE table_with_index_rep_identity REPLICA IDENTITY USING INDEX uqx;
CLUSTER table_with_index_rep_identity USING uqx;
SELECT create_distributed_table('table_with_index_rep_identity', 'key', colocate_with:='sensors');
-- END: Create co-located distributed and reference tables.

-- BEGIN : Create Foreign key constraints.
ALTER TABLE sensors ADD CONSTRAINT fkey_table_to_dist FOREIGN KEY (measureid) REFERENCES colocated_dist_table(measureid);
-- END : Create Foreign key constraints.

-- BEGIN : Load data into tables.
INSERT INTO reference_table SELECT i FROM generate_series(0,1000)i;
INSERT INTO colocated_dist_table(measureid, value, col_todrop) SELECT i,'Value',i FROM generate_series(0,1000)i;
INSERT INTO sensors SELECT i, '2020-01-05', '{}', 11011.10, 'A', 'I <3 Citus' FROM generate_series(0,1000)i;

ALTER TABLE colocated_dist_table DROP COLUMN col_todrop;

SELECT COUNT(*) FROM sensors;
SELECT COUNT(*) FROM reference_table;
SELECT COUNT(*) FROM colocated_dist_table;
-- END: Load data into tables.

-- BEGIN : Display current state.
SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
  FROM pg_dist_shard AS shard
  INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
  INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
  INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
  WHERE node.noderole = 'primary' AND (logicalrelid = 'sensors'::regclass OR logicalrelid = 'colocated_dist_table'::regclass OR logicalrelid = 'table_with_index_rep_identity'::regclass)
  ORDER BY logicalrelid, shardminvalue::BIGINT;

\c - - - :worker_1_port
    SET search_path TO "citus_split_test_schema", public, pg_catalog;
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like 'sensors_%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'sensors_%' ORDER BY 1,2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'table_with_index_rep_identity_%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema')
    )
    ORDER BY stxname ASC;

\c - - - :worker_2_port
    SET search_path TO "citus_split_test_schema", public, pg_catalog;
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like 'sensors_%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'sensors_%' ORDER BY 1,2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'table_with_index_rep_identity_%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema')
    )
    ORDER BY stxname ASC;
-- END : Display current state

-- BEGIN : Move one shard before we split it.
\c - postgres - :master_port
SET ROLE test_shard_split_role;
SET search_path TO "citus_split_test_schema";
SET citus.next_shard_id TO 8981007;

SELECT citus_move_shard_placement(8981000, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='force_logical');
-- END : Move one shard before we split it.

SELECT public.wait_for_resource_cleanup();

-- BEGIN : Set node id variables
SELECT nodeid AS worker_1_node FROM pg_dist_node WHERE nodeport=:worker_1_port \gset
SELECT nodeid AS worker_2_node FROM pg_dist_node WHERE nodeport=:worker_2_port \gset
-- END   : Set node id variables

-- BEGIN : Split two shards : One with move and One without move.
-- Perform 2 way split
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981000,
    ARRAY['-1073741824'],
    ARRAY[:worker_1_node, :worker_2_node],
    'force_logical');

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

-- Perform 3 way split
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981001,
    ARRAY['536870911', '1610612735'],
    ARRAY[:worker_1_node, :worker_1_node, :worker_2_node],
    'force_logical');
-- END : Split two shards : One with move and One without move.

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

-- BEGIN : Move a shard post split.
SELECT citus_move_shard_placement(8981007, 'localhost', :worker_1_port, 'localhost', :worker_2_port, shard_transfer_mode:='block_writes');
-- END : Move a shard post split.

SELECT public.wait_for_resource_cleanup();

-- BEGIN : Display current state.
SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
  FROM pg_dist_shard AS shard
  INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
  INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
  INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
  WHERE node.noderole = 'primary' AND (logicalrelid = 'sensors'::regclass OR logicalrelid = 'colocated_dist_table'::regclass OR logicalrelid = 'table_with_index_rep_identity'::regclass)
  ORDER BY logicalrelid, shardminvalue::BIGINT;

\c - - - :worker_1_port
    SET search_path TO "citus_split_test_schema", public, pg_catalog;
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like 'sensors_%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'sensors_%' ORDER BY 1,2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'table_with_index_rep_identity_%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema')
    )
    ORDER BY stxname ASC;

\c - - - :worker_2_port
    SET search_path TO "citus_split_test_schema", public, pg_catalog;
    SET citus.show_shards_for_app_name_prefixes = '*';
    SELECT tbl.relname, fk."Constraint", fk."Definition"
            FROM pg_catalog.pg_class tbl
            JOIN public.table_fkeys fk on tbl.oid = fk.relid
            WHERE tbl.relname like 'sensors_%'
            ORDER BY 1, 2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'sensors_%' ORDER BY 1,2;
    SELECT tablename, indexdef FROM pg_indexes WHERE tablename like 'table_with_index_rep_identity_%' ORDER BY 1,2;
    SELECT stxname FROM pg_statistic_ext
    WHERE stxnamespace IN (
        SELECT oid
        FROM pg_namespace
        WHERE nspname IN ('citus_split_test_schema')
    )
    ORDER BY stxname ASC;
-- END : Display current state

-- BEGIN: Should be able to change/drop constraints
\c - postgres - :master_port
SET ROLE test_shard_split_role;
SET search_path TO "citus_split_test_schema";
ALTER INDEX index_on_sensors RENAME TO index_on_sensors_renamed;
ALTER INDEX index_on_sensors_renamed ALTER COLUMN 1 SET STATISTICS 200;
DROP STATISTICS stats_on_sensors;
DROP INDEX index_on_sensors_renamed;
ALTER TABLE sensors DROP CONSTRAINT fkey_table_to_dist;
-- END: Should be able to change/drop constraints

-- BEGIN: Split second time on another schema
SET search_path TO public;
SET citus.next_shard_id TO 8981031;
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981007,
    ARRAY['-2100000000'],
    ARRAY[:worker_1_node, :worker_2_node],
    'force_logical');

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

SET search_path TO "citus_split_test_schema";
SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
  FROM pg_dist_shard AS shard
  INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
  INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
  INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
  WHERE node.noderole = 'primary' AND (logicalrelid = 'sensors'::regclass OR logicalrelid = 'colocated_dist_table'::regclass OR logicalrelid = 'table_with_index_rep_identity'::regclass)
  ORDER BY logicalrelid, shardminvalue::BIGINT;
-- END: Split second time on another schema

-- BEGIN: Create a co-located table with no replica identity.
CREATE TABLE table_no_rep_id (measureid integer);
SELECT create_distributed_table('table_no_rep_id', 'measureid', colocate_with:='sensors');
-- END: Create a co-located table with no replica identity.

-- BEGIN: Split a shard with shard_transfer_mode='auto' and with a colocated table with no replica identity
SET citus.next_shard_id TO 8981041;
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981031,
    ARRAY['-2120000000'],
    ARRAY[:worker_1_node, :worker_2_node]);

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
  FROM pg_dist_shard AS shard
  INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
  INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
  INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
  WHERE node.noderole = 'primary' AND (logicalrelid = 'sensors'::regclass OR logicalrelid = 'colocated_dist_table'::regclass OR logicalrelid = 'table_with_index_rep_identity'::regclass)
  ORDER BY logicalrelid, shardminvalue::BIGINT;
-- END: Split a shard with shard_transfer_mode='auto' and with a colocated table with no replica identity

-- BEGIN: Drop the co-located table with no replica identity.
DROP TABLE table_no_rep_id;
-- END: Drop the co-located table with no replica identity.

-- BEGIN: Split a shard with shard_transfer_mode='auto' and with all colocated tables has replica identity
SET citus.next_shard_id TO 8981041;
SELECT pg_catalog.citus_split_shard_by_split_points(
    8981031,
    ARRAY['-2120000000'],
    ARRAY[:worker_1_node, :worker_2_node],
    'auto');

-- BEGIN: Perform deferred cleanup.
SELECT public.wait_for_resource_cleanup();
-- END: Perform deferred cleanup.

SELECT shard.shardid, logicalrelid, shardminvalue, shardmaxvalue, nodename, nodeport
  FROM pg_dist_shard AS shard
  INNER JOIN pg_dist_placement placement ON shard.shardid = placement.shardid
  INNER JOIN pg_dist_node       node     ON placement.groupid = node.groupid
  INNER JOIN pg_catalog.pg_class cls     ON shard.logicalrelid = cls.oid
  WHERE node.noderole = 'primary' AND (logicalrelid = 'sensors'::regclass OR logicalrelid = 'colocated_dist_table'::regclass OR logicalrelid = 'table_with_index_rep_identity'::regclass)
  ORDER BY logicalrelid, shardminvalue::BIGINT;
-- END: Split a shard with shard_transfer_mode='auto' and with all colocated tables has replica identity

-- BEGIN: Validate Data Count
SELECT COUNT(*) FROM sensors;
SELECT COUNT(*) FROM reference_table;
SELECT COUNT(*) FROM colocated_dist_table;
-- END: Validate Data Count

--BEGIN : Cleanup
\c - postgres - :master_port
-- make sure we don't have any replication objects leftover on the workers
SELECT run_command_on_workers($$SELECT count(*) FROM pg_replication_slots$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_publication$$);
SELECT run_command_on_workers($$SELECT count(*) FROM pg_subscription$$);

ALTER SYSTEM RESET citus.defer_shard_delete_interval;
SELECT pg_reload_conf();
DROP SCHEMA "citus_split_test_schema" CASCADE;
DROP ROLE test_shard_split_role;
--END : Cleanup
