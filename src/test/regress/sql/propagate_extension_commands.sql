CREATE SCHEMA "extension'test";

-- use  a schema name with escape character
SET search_path TO "extension'test";

-- create an extension on the given search_path
-- the extension is on contrib, so should be avaliable for the regression tests
CREATE EXTENSION seg;

--  make sure that both the schema and the extension is distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'seg');
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_namespace WHERE nspname = 'extension''test');

CREATE TABLE test_table (key int, value seg);
SELECT create_distributed_table('test_table', 'key');

--  make sure that the table is also distributed now
SELECT count(*) from pg_dist_partition where logicalrelid='extension''test.test_table'::regclass;

CREATE TYPE two_segs AS (seg_1 seg, seg_2 seg);

-- verify that the type that depends on the extension is also marked as distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE typname = 'two_segs' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'extension''test'));

-- now try to run CREATE EXTENSION within a transction block, all should work fine
BEGIN;
	CREATE EXTENSION isn WITH SCHEMA public;

  -- now, try create a reference table relying on the data types
  -- this should not succeed as we do not distribute extension commands within transaction blocks
	CREATE TABLE dist_table (key int, value public.issn);
	SELECT create_distributed_table('dist_table', 'key');

	-- we can even run queries (sequentially) over the distributed table
	SELECT * FROM dist_table;
	INSERT INTO dist_table VALUES (1, public.issn('1436-4522'));
	INSERT INTO dist_table SELECT * FROM dist_table RETURNING *;
COMMIT;

-- make sure that the extension is distributed even if we run create extension in a transaction block
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);



CREATE TABLE ref_table (a public.issn);
-- now, create a reference table relying on the data types
SELECT create_reference_table('ref_table');

-- now,  drop the extension, recreate it with an older version and update it to latest version
SET client_min_messages TO WARNING;
DROP EXTENSION isn CASCADE;
CREATE EXTENSION isn WITH VERSION "1.1";
RESET client_min_messages;

-- before updating the version, ensure the current version
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);

-- now, update to a newer version
ALTER EXTENSION isn UPDATE TO '1.2';

-- show that ALTER EXTENSION is propagated
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);

-- before changing the schema, ensure the current schmea
SELECT run_command_on_workers($$SELECT nspname from pg_namespace where oid=(SELECT extnamespace FROM pg_extension WHERE extname = 'isn')$$);

-- now change the schema
ALTER EXTENSION isn SET SCHEMA public;

-- switch back to public schema as we set extension's schema to public
SET search_path TO public;

-- make sure that the extension is distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- show that the ALTER EXTENSION command is propagated
SELECT run_command_on_workers($$SELECT nspname from pg_namespace where oid=(SELECT extnamespace FROM pg_extension WHERE extname = 'isn')$$);

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- drop the extension finally
DROP EXTENSION isn CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- now make sure that the reference tables depending on an extension can be succesfully created.
-- we should also ensure that we replicate this reference table (and hence the extension)
-- to new nodes after calling master_activate_node.

-- now, first drop seg and existing objects before next test
SET client_min_messages TO WARNING;
DROP EXTENSION seg CASCADE;

-- but as we have only 2 ports in postgresql tests, let's remove one of the nodes first
-- before remove, first remove the existing relations (due to the other tests)

DROP SCHEMA "extension'test" CASCADE;
RESET client_min_messages;
SELECT 1 from master_remove_node(:'worker_2_host', :worker_2_port);

-- then create the extension
CREATE EXTENSION seg;

-- show that the extension is created on existing worker
SELECT run_command_on_workers($$SELECT count(extnamespace) FROM pg_extension WHERE extname = 'seg'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'seg'$$);

-- now create the reference table
CREATE TABLE ref_table_2 (x seg);
SELECT create_reference_table('ref_table_2');

-- and add the other node
SELECT 1 from master_add_node(:'worker_2_host', :worker_2_port);

-- show that the extension is created on both existing and new node
SELECT run_command_on_workers($$SELECT count(extnamespace) FROM pg_extension WHERE extname = 'seg'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'seg'$$);

-- and similarly check for the reference table
select count(*) from pg_dist_partition where partmethod='n' and logicalrelid='ref_table_2'::regclass;
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='ref_table_2'::regclass;
DROP TABLE ref_table_2;
-- now test create extension in another transaction block but rollback this time
BEGIN;
	CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
ROLLBACK;

-- at the end of the transaction block, we did not create isn extension in coordinator or worker nodes as we rollback'ed
-- make sure that the extension is not distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- and the extension does not exist on workers
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);

-- give a notice for the following commands saying that it is not
-- propagated to the workers. the user should run it manually on the workers
CREATE TABLE t1 (A int);
CREATE VIEW v1 AS select * from t1;

ALTER EXTENSION seg ADD VIEW v1;
ALTER EXTENSION seg DROP VIEW v1;
DROP VIEW v1;
DROP TABLE t1;

-- drop multiple extensions at the same time
CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
-- let's create another extension locally
set citus.enable_ddl_propagation to 'off';
CREATE EXTENSION pg_buffercache;
set citus.enable_ddl_propagation to 'on';

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
DROP EXTENSION pg_buffercache, isn CASCADE;
SELECT count(*) FROM pg_extension WHERE extname IN ('pg_buffercache', 'isn');
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;

-- drop extension should just work
DROP EXTENSION seg CASCADE;

SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'seg');
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'seg'$$);

-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- make sure that the extension is not avaliable anymore as a distributed object
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('seg', 'isn'));

CREATE SCHEMA "extension'test";
SET search_path TO "extension'test";
-- check restriction for sequential execution
-- enable it and see that create command errors but continues its execution by changing citus.multi_shard_modify_mode TO 'off

BEGIN;
	CREATE TABLE some_random_table (a int);
	SELECT create_distributed_table('some_random_table', 'a');
	CREATE EXTENSION seg;
	CREATE TABLE some_random_table_2 (a int, b seg);
	SELECT create_distributed_table('some_random_table_2', 'a');
ROLLBACK;

-- show that the CREATE EXTENSION command propagated even if the transaction
-- block is rollbacked, that's a shortcoming of dependency creation logic
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'seg'$$);

-- drop the schema and all the objects
SET client_min_messages TO WARNING;
DROP SCHEMA "extension'test" CASCADE;

-- recreate for the next tests
CREATE SCHEMA "extension'test";

-- use  a schema name with escape character
SET search_path TO "extension'test";

RESET client_min_messages;

-- remove the node, we'll add back again
SELECT 1 from master_remove_node(:'worker_2_host', :worker_2_port);

-- now, create a type that depends on another type, which
-- finally depends on an extension
BEGIN;
	SET citus.shard_replication_factor TO 1;
	CREATE EXTENSION seg;
	CREATE EXTENSION isn;
	CREATE TYPE test_type AS (a int, b seg);
	CREATE TYPE test_type_2 AS (a int, b test_type);

	CREATE TABLE t2 (a int, b test_type_2, c issn);
	SELECT create_distributed_table('t2', 'a');

	CREATE TYPE test_type_3 AS (a int, b test_type, c issn);
	CREATE TABLE t3 (a int, b test_type_3);
	SELECT create_reference_table('t3');

COMMIT;

-- add the node back
SELECT 1 from master_add_node(:'worker_2_host', :worker_2_port);

-- make sure that both extensions are created on both nodes
SELECT count(*) FROM citus.pg_dist_object WHERE objid IN (SELECT oid FROM pg_extension WHERE extname IN ('seg', 'isn'));
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname IN ('seg', 'isn')$$);

-- drop the schema and all the objects
SET client_min_messages TO WARNING;
DROP SCHEMA "extension'test" CASCADE;
