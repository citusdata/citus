--
-- DROP_PARTITIONED_TABLE
--
-- Tests to make sure that we properly drop distributed partitioned tables
--

SET citus.next_shard_id TO 720000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA drop_partitioned_table;
SET search_path = drop_partitioned_table;

-- create a function that allows us to see the values of
-- original and normal values for each dropped table
-- coming from pg_event_trigger_dropped_objects()
-- for now the only case where we can distinguish a
-- dropped partition because of its dropped parent
-- is when both values are false: check citus_drop_trigger

CREATE FUNCTION check_original_normal_values()
    RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE
    v_obj record;
BEGIN
    FOR v_obj IN SELECT * FROM pg_event_trigger_dropped_objects()
        WHERE object_type IN ('table', 'foreign table')
    LOOP
	    RAISE NOTICE 'dropped object: %.% original: % normal: %',
            v_obj.schema_name,
            v_obj.object_name,
            v_obj.original,
            v_obj.normal;
    END LOOP;
END;
$$;

CREATE EVENT TRIGGER new_trigger_for_drops
   ON sql_drop
   EXECUTE FUNCTION check_original_normal_values();

-- create a view printing the same output as \d for this test's schemas
-- since \d output is not guaranteed to be consistent between different PG versions etc
CREATE VIEW tables_info AS
SELECT n.nspname as "Schema",
    c.relname as "Name",
    CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' END as "Type",
    pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
    LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_user u ON u.usesysid = c.relowner
WHERE n.nspname IN ('drop_partitioned_table', 'schema1')
    AND c.relkind IN ('r','p')
ORDER BY 1, 2;

SET citus.next_shard_id TO 721000;

-- CASE 1
-- Dropping the parent table
CREATE TABLE parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
SELECT create_distributed_table('parent','x');

SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP TABLE parent;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET search_path = drop_partitioned_table;
SET citus.next_shard_id TO 722000;

-- CASE 2
-- Dropping the parent table, but including children in the DROP command
CREATE TABLE parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
SELECT create_distributed_table('parent','x');

SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP TABLE child1, parent, child2;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET search_path = drop_partitioned_table;
SET citus.next_shard_id TO 723000;

-- CASE 3
-- DROP OWNED BY role1; Only parent is owned by role1, children are owned by another owner
SET client_min_messages TO ERROR;
CREATE ROLE role1;
SELECT 1 FROM run_command_on_workers('CREATE ROLE role1');
RESET client_min_messages;
GRANT ALL ON SCHEMA drop_partitioned_table TO role1;
SET ROLE role1;
CREATE TABLE drop_partitioned_table.parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
RESET ROLE;
CREATE TABLE child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
SELECT create_distributed_table('parent','x');

SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP OWNED BY role1;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET search_path = drop_partitioned_table;
SET citus.next_shard_id TO 724000;

-- CASE 4
-- DROP OWNED BY role1; Parent and children are owned by role1
GRANT ALL ON SCHEMA drop_partitioned_table TO role1;
SET ROLE role1;
CREATE TABLE drop_partitioned_table.parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE drop_partitioned_table.child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE drop_partitioned_table.parent ATTACH PARTITION drop_partitioned_table.child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE drop_partitioned_table.child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE drop_partitioned_table.parent ATTACH PARTITION drop_partitioned_table.child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
RESET ROLE;
SELECT create_distributed_table('parent','x');

SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP OWNED BY role1;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET search_path = drop_partitioned_table;
SET citus.next_shard_id TO 725000;
REVOKE ALL ON SCHEMA drop_partitioned_table FROM role1;
DROP ROLE role1;
SELECT run_command_on_workers('DROP ROLE IF EXISTS role1');

-- CASE 5
-- DROP SCHEMA schema1 CASCADE; Parent is in schema1, children are in another schema
CREATE SCHEMA schema1;
CREATE TABLE schema1.parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE schema1.parent ATTACH PARTITION child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE schema1.parent ATTACH PARTITION child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
SELECT create_distributed_table('schema1.parent','x');

SET search_path = drop_partitioned_table, schema1;
SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP SCHEMA schema1 CASCADE;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table, schema1;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET citus.next_shard_id TO 726000;
-- CASE 6
-- DROP SCHEMA schema1 CASCADE; Parent and children are in schema1
CREATE SCHEMA schema1;
CREATE TABLE schema1.parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE schema1.child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE schema1.parent ATTACH PARTITION schema1.child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
CREATE TABLE schema1.child2 (x text, t timestamptz DEFAULT now());
ALTER TABLE schema1.parent ATTACH PARTITION schema1.child2 FOR VALUES FROM ('2021-06-30') TO ('2021-07-01');
SELECT create_distributed_table('schema1.parent','x');

SET search_path = drop_partitioned_table, schema1;
SELECT * FROM drop_partitioned_table.tables_info;
\set VERBOSITY terse
DROP SCHEMA schema1 CASCADE;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :worker_1_port
SET search_path = drop_partitioned_table, schema1;
SELECT * FROM drop_partitioned_table.tables_info;

\c - - - :master_port
SET search_path = drop_partitioned_table;

-- Check that we actually skip sending remote commands to skip shards
SET citus.shard_count TO 1;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 727000;
ALTER SEQUENCE pg_catalog.pg_dist_colocationid_seq RESTART 1344400;
DROP EVENT TRIGGER new_trigger_for_drops;

-- Case 1 - we should skip
CREATE TABLE parent (x text, t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
CREATE TABLE child1 (x text, t timestamptz DEFAULT now());
ALTER TABLE parent ATTACH PARTITION child1 FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
SELECT create_distributed_table('parent','x');
BEGIN;
SET citus.log_remote_commands TO on;
DROP TABLE parent;
ROLLBACK;

-- Case 2 - we shouldn't skip
BEGIN;
SET citus.log_remote_commands TO on;
DROP TABLE parent, child1;
ROLLBACK;

DROP SCHEMA drop_partitioned_table CASCADE;
SET search_path TO public;

-- dropping the schema should drop the metadata on the workers
CREATE SCHEMA partitioning_schema;
SET search_path TO partitioning_schema;

CREATE TABLE part_table (
      col timestamp
  ) PARTITION BY RANGE (col);

CREATE TABLE part_table_1
  PARTITION OF part_table
  FOR VALUES FROM ('2010-01-01') TO ('2015-01-01');

SELECT create_distributed_table('part_table', 'col');

-- show we have pg_dist_partition entries on the workers
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_partition where exists(select * from pg_class where pg_class.oid=pg_dist_partition.logicalrelid AND relname ILIKE '%part_table%');$$);
-- show we have pg_dist_object entries on the workers
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_object as obj where classid = 1259 AND exists(select * from pg_class where pg_class.oid=obj.objid AND relname ILIKE '%part_table%');$$);

DROP SCHEMA partitioning_schema CASCADE;

-- show we don't have pg_dist_partition entries on the workers after dropping the schema
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_partition where exists(select * from pg_class where pg_class.oid=pg_dist_partition.logicalrelid AND relname ILIKE '%part_table%');$$);

-- show we don't have pg_dist_object entries on the workers after dropping the schema
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_object as obj where classid = 1259 AND exists(select * from pg_class where pg_class.oid=obj.objid AND relname ILIKE '%part_table%');$$);

-- dropping the parent should drop the metadata on the workers
CREATE SCHEMA partitioning_schema;
SET search_path TO partitioning_schema;

CREATE TABLE part_table (
      col timestamp
  ) PARTITION BY RANGE (col);

CREATE TABLE part_table_1
  PARTITION OF part_table
  FOR VALUES FROM ('2010-01-01') TO ('2015-01-01');

SELECT create_distributed_table('part_table', 'col');

DROP TABLE part_table;

-- show we don't have pg_dist_partition entries on the workers after dropping the parent
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_partition where exists(select * from pg_class where pg_class.oid=pg_dist_partition.logicalrelid AND relname ILIKE '%part_table%');$$);

-- show we don't have pg_dist_object entries on the workers after dropping the parent
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_object as obj where classid = 1259 AND exists(select * from pg_class where pg_class.oid=obj.objid AND relname ILIKE '%part_table%');$$);

SET search_path TO partitioning_schema;

CREATE TABLE part_table (
      col timestamp
  ) PARTITION BY RANGE (col);

CREATE TABLE part_table_1
  PARTITION OF part_table
  FOR VALUES FROM ('2010-01-01') TO ('2015-01-01');

SELECT create_distributed_table('part_table', 'col');

DROP TABLE part_table_1;

-- show we have pg_dist_partition entries for the parent on the workers after dropping the partition
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_partition where exists(select * from pg_class where pg_class.oid=pg_dist_partition.logicalrelid AND relname ILIKE '%part_table%');$$);

-- show we have pg_dist_object entries for the parent on the workers after dropping the partition
SELECT run_command_on_workers($$SELECT count(*) FROM  pg_dist_object as obj where classid = 1259 AND exists(select * from pg_class where pg_class.oid=obj.objid AND relname ILIKE '%part_table%');$$);

-- clean-up
DROP SCHEMA partitioning_schema CASCADE;
