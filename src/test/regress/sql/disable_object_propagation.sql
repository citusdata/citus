SET citus.next_shard_id TO 20030000;
SET citus.enable_object_propagation TO false; -- all tests here verify old behaviour without distributing types,functions,etc automatically

CREATE USER typeowner_for_disabled_object_propagation_guc;
CREATE SCHEMA disabled_object_propagation;
CREATE SCHEMA disabled_object_propagation2;
SET search_path TO disabled_object_propagation;

-- verify the table gets created, which requires schema distribution to still work
CREATE TABLE t1 (a int PRIMARY KEY , b int);
SELECT create_distributed_table('t1','a');

-- verify types are not created, preventing distributed tables to be created unless created manually on the workers
CREATE TYPE tt1 AS (a int , b int);
CREATE TABLE t2 (a int PRIMARY KEY, b tt1);
SELECT create_distributed_table('t2', 'a');
SELECT 1 FROM run_command_on_workers($$
    BEGIN;
    SET LOCAL citus.enable_ddl_propagation TO off;
    CREATE TYPE disabled_object_propagation.tt1 AS (a int , b int);
    COMMIT;
$$);
SELECT create_distributed_table('t2', 'a');

-- verify enum types are not created, preventing distributed tables to be created unless created manually on the workers
CREATE TYPE tt2 AS ENUM ('a', 'b');
CREATE TABLE t3 (a int PRIMARY KEY, b tt2);
SELECT create_distributed_table('t3', 'a');
SELECT 1 FROM  run_command_on_workers($$
    BEGIN;
    SET LOCAL citus.enable_ddl_propagation TO off;
    CREATE TYPE disabled_object_propagation.tt2 AS ENUM ('a', 'b');
    COMMIT;
$$);
SELECT create_distributed_table('t3', 'a');

-- verify ALTER TYPE statements are not propagated for types, even though they are marked distributed
BEGIN;
-- object propagation is turned off after xact finished, type is already marked distributed by then
SET LOCAL citus.enable_object_propagation TO on;
CREATE TYPE tt3 AS (a int, b int);
CREATE TABLE t4 (a int PRIMARY KEY, b tt3);
SELECT create_distributed_table('t4','a');
DROP TABLE t4; -- as long as the table is using the type some operations are hard to force
COMMIT;

-- verify the type is distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = 'disabled_object_propagation.tt3'::regtype::oid;

ALTER TYPE tt3 ADD ATTRIBUTE c int, DROP ATTRIBUTE b, ALTER ATTRIBUTE a SET DATA TYPE text COLLATE "POSIX";
ALTER TYPE tt3 OWNER TO typeowner_for_disabled_object_propagation_guc;
ALTER TYPE tt3 RENAME ATTRIBUTE c TO d;
ALTER TYPE tt3 RENAME TO tt4;
ALTER TYPE tt4 SET SCHEMA disabled_object_propagation2;
DROP TYPE disabled_object_propagation2.tt4;

-- verify no changes have been made to the type on the remote servers. tt3 is used as a name since the rename should not have been propagated
SELECT run_command_on_workers($$
SELECT row(nspname, typname, usename)
  FROM pg_type
  JOIN pg_user ON (typowner = usesysid)
  JOIN pg_namespace ON (pg_namespace.oid = typnamespace)
 WHERE typname = 'tt3';
$$);

SELECT run_command_on_workers($$
  SELECT row(pg_type.typname, string_agg(attname || ' ' || atttype.typname, ', ' ORDER BY attnum))
    FROM pg_attribute
    JOIN pg_class ON (attrelid = pg_class.oid)
    JOIN pg_type ON (pg_class.reltype = pg_type.oid)
    JOIN pg_type AS atttype ON (atttypid = atttype.oid)
   WHERE pg_type.typname = 'tt3'
GROUP BY pg_type.typname;
$$);

-- suppress any warnings during cleanup
SET client_min_messages TO error;
RESET citus.enable_object_propagation;
DROP SCHEMA disabled_object_propagation CASCADE;
DROP SCHEMA disabled_object_propagation2 CASCADE;
DROP USER typeowner_for_disabled_object_propagation_guc;

