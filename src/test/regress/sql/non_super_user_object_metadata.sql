SET citus.enable_ddl_propagation TO OFF;

CREATE SCHEMA local_schema;
SET search_path TO local_schema;

-- Create type and function that depends on it
CREATE TYPE test_type AS (f1 int, f2 text);
CREATE FUNCTION test_function(int) RETURNS test_type
    AS $$ SELECT $1, CAST($1 AS text) || ' is text' $$
    LANGUAGE SQL;

-- Create various objects
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');

-- Create a sequence under a different schema
CREATE SCHEMA test_sequence_schema;
CREATE SEQUENCE test_sequence_schema.test_sequence;

-- show that none of the objects above are marked as distributed
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema'::regnamespace::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.mood'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_type'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'test_sequence_schema.test_sequence'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_function'::regproc::oid;

SET client_min_messages TO ERROR;
CREATE USER non_super_user_test_user;
SELECT 1 FROM run_command_on_workers($$CREATE USER non_super_user_test_user;$$);
RESET client_min_messages;

GRANT ALL ON SCHEMA local_schema TO non_super_user_test_user;
GRANT ALL ON SCHEMA test_sequence_schema TO non_super_user_test_user;

SET ROLE non_super_user_test_user;
SET search_path TO local_schema;
CREATE TABLE dist_table(a int, b mood, c test_type, d int DEFAULT nextval('test_sequence_schema.test_sequence'), e bigserial);

-- Citus requires that user must own the dependent sequence
-- https://github.com/citusdata/citus/issues/5494
SELECT create_distributed_table('local_schema.dist_table', 'a');

-- Citus requires that user must own the function to distribute
SELECT create_distributed_function('test_function(int)');

RESET ROLE;
SET search_path TO local_schema;
ALTER SEQUENCE test_sequence_schema.test_sequence OWNER TO non_super_user_test_user;
ALTER FUNCTION test_function(int) OWNER TO non_super_user_test_user;

SET ROLE non_super_user_test_user;
SET search_path TO local_schema;

-- Show that we can distribute table and function after
-- having required ownerships
SELECT create_distributed_table('dist_table', 'a');
SELECT create_distributed_function('test_function(int)');

-- Create and distribute plpgsql extension's function
CREATE OR REPLACE FUNCTION plpgsql_dist_function(text)
RETURNS void
LANGUAGE plpgsql AS
$$
    BEGIN
        RAISE NOTICE '%', $1;
    END;
$$;

SELECT create_distributed_function('plpgsql_dist_function(text)');

-- show that schema, types, function and sequence has marked as distributed
-- on the coordinator node
RESET ROLE;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema'::regnamespace::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'test_sequence_schema'::regnamespace::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.mood'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_type'::regtype::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'test_sequence_schema.test_sequence'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.dist_table_e_seq'::regclass::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_function'::regproc::oid;
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.plpgsql_dist_function'::regproc::oid;

-- show those objects marked as distributed on metadata worker node as well
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema'::regnamespace::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'test_sequence_schema'::regnamespace::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.mood'::regtype::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_type'::regtype::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'test_sequence_schema.test_sequence'::regclass::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.dist_table_e_seq'::regclass::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.test_function'::regproc::oid;$$) ORDER BY 1,2;
SELECT * FROM run_command_on_workers($$SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object where objid = 'local_schema.plpgsql_dist_function'::regproc::oid;$$) ORDER BY 1,2;

-- Show that extension plpgsql is also marked as distributed as a dependency of plpgsl_dist_function
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{plpgsql}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{plpgsql}%';$$) ORDER BY 1,2;

-- show that schema is owned by the superuser
SELECT rolname FROM pg_roles JOIN pg_namespace ON(pg_namespace.nspowner = pg_roles.oid) WHERE nspname = 'local_schema';
SELECT * FROM run_command_on_workers($$SELECT rolname FROM pg_roles JOIN pg_namespace ON(pg_namespace.nspowner = pg_roles.oid) WHERE nspname = 'local_schema';$$) ORDER BY 1,2;

-- show that types are owned by the superuser
SELECT DISTINCT(rolname) FROM pg_roles JOIN pg_type ON(pg_type.typowner = pg_roles.oid) WHERE typname IN ('test_type', 'mood');
SELECT * FROM run_command_on_workers($$SELECT DISTINCT(rolname) FROM pg_roles JOIN pg_type ON(pg_type.typowner = pg_roles.oid) WHERE typname IN ('test_type', 'mood');$$) ORDER BY 1,2;

-- show that table is owned by the non_super_user_test_user
SELECT rolname FROM pg_roles JOIN pg_class ON(pg_class.relowner = pg_roles.oid) WHERE relname = 'dist_table';
SELECT * FROM run_command_on_workers($$SELECT rolname FROM pg_roles JOIN pg_class ON(pg_class.relowner = pg_roles.oid) WHERE relname = 'dist_table'$$) ORDER BY 1,2;

SET ROLE non_super_user_test_user;
SET search_path TO local_schema;

-- ensure we can load data
INSERT INTO dist_table VALUES (1, 'sad', (1,'onder')::test_type),
							  (2, 'ok', (1,'burak')::test_type),
							  (3, 'happy', (1,'marco')::test_type);

SELECT a, b, c , d FROM dist_table ORDER BY 1,2,3,4;

-- Show that dropping the table removes the dependent sequence from pg_dist_object
-- on both coordinator and metadata worker nodes when ddl propagation is on
SET citus.enable_ddl_propagation TO ON;
DROP TABLE dist_table CASCADE;

RESET ROLE;
SET search_path TO local_schema;
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%dist_table_e_seq%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%dist_table_e_seq%';$$) ORDER BY 1,2;

-- Show that altering the function's schema marks the schema distributed
CREATE SCHEMA schema_to_prop_with_function;
ALTER FUNCTION test_function SET SCHEMA schema_to_prop_with_function;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{schema_to_prop_with_function}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{schema_to_prop_with_function}%';$$) ORDER BY 1,2;

-- Show that dropping the function removes the metadata from pg_dist_object
-- on both coordinator and metadata worker node
DROP FUNCTION schema_to_prop_with_function.test_function;
SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%test_function%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%test_function%';$$) ORDER BY 1,2;

-- Show that altering the type's schema marks the schema distributed
CREATE SCHEMA schema_to_prop_with_type;
ALTER TYPE test_type SET SCHEMA schema_to_prop_with_type;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{schema_to_prop_with_type}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{schema_to_prop_with_type}%';$$) ORDER BY 1,2;

-- Show that dropping type removes the metadata from pg_dist_object
-- on both coordinator and metadata worker node
DROP TYPE mood CASCADE;
DROP TYPE schema_to_prop_with_type.test_type CASCADE;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%test_type%' or obj_identifier::text like '%mood%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%test_type%' or obj_identifier::text like '%mood%'$$) ORDER BY 1,2;

-- Show that distributed function related metadata are also propagated
set citus.shard_replication_factor to 1;

CREATE TABLE metadata_dist_test_table (a int, b int);
SELECT create_distributed_table('metadata_dist_test_table', 'a');
CREATE OR REPLACE PROCEDURE metadata_dist_test_proc(dist_key integer, dist_key_2 integer)
LANGUAGE plpgsql
AS $$ DECLARE
res INT := 0;
BEGIN
    INSERT INTO metadata_dist_test_table VALUES (dist_key);
    SELECT count(*) INTO res FROM metadata_dist_test_table;
    RAISE NOTICE 'Res: %', res;
COMMIT;
END;$$;

-- create a distributed function and show its distribution_argument_index
SELECT create_distributed_function('metadata_dist_test_proc(integer, integer)', 'dist_key', 'metadata_dist_test_table');
SELECT distribution_argument_index FROM pg_catalog.pg_dist_object WHERE objid = 'metadata_dist_test_proc'::regproc;
SELECT * FROM run_command_on_workers($$SELECT distribution_argument_index FROM pg_catalog.pg_dist_object WHERE objid = 'local_schema.metadata_dist_test_proc'::regproc;$$) ORDER BY 1,2;

-- re-distribute and show that now the distribution_argument_index is updated on both the coordinator and workers
SELECT create_distributed_function('metadata_dist_test_proc(integer, integer)', 'dist_key_2', 'metadata_dist_test_table');
SELECT distribution_argument_index FROM pg_catalog.pg_dist_object WHERE objid = 'metadata_dist_test_proc'::regproc;
SELECT * FROM run_command_on_workers($$ SELECT distribution_argument_index FROM pg_catalog.pg_dist_object WHERE objid = 'local_schema.metadata_dist_test_proc'::regproc;$$) ORDER BY 1,2;

-- Show that the schema is dropped on worker node as well
DROP SCHEMA local_schema CASCADE;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{local_schema}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{local_schema}%';$$) ORDER BY 1,2;

-- Show that extension and dependent sequence also created and marked as distributed
CREATE SCHEMA extension_schema;
CREATE EXTENSION ltree WITH SCHEMA extension_schema;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{extension_schema}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{extension_schema}%';$$) ORDER BY 1,2;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{ltree}%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%{ltree}%';$$) ORDER BY 1,2;

-- Show that dropping a distributed table drops the pg_dist_object entry on worker
CREATE TABLE extension_schema.table_to_check_object(id int);
SELECT create_distributed_table('extension_schema.table_to_check_object', 'id');

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%table_to_check_object%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%table_to_check_object%';$$) ORDER BY 1,2;

DROP TABLE extension_schema.table_to_check_object;

SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%table_to_check_object%';
SELECT * FROM run_command_on_workers($$SELECT * FROM (SELECT pg_identify_object_as_address(classid, objid, objsubid) as obj_identifier from pg_catalog.pg_dist_object) as obj_identifiers where obj_identifier::text like '%table_to_check_object%';$$) ORDER BY 1,2;

-- Revert the settings for following tests
RESET citus.enable_ddl_propagation;
RESET citus.shard_replication_factor;
