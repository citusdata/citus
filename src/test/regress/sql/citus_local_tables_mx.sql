\set VERBOSITY terse

SET citus.next_shard_id TO 1508000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA citus_local_tables_mx;
SET search_path TO citus_local_tables_mx;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

--------------
-- triggers --
--------------

CREATE TABLE citus_local_table (value int);
SELECT create_citus_local_table('citus_local_table');

-- first stop metadata sync to worker_1
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);

CREATE FUNCTION dummy_function() RETURNS trigger AS $dummy_function$
BEGIN
    RAISE EXCEPTION 'a trigger that throws this exception';
END;
$dummy_function$ LANGUAGE plpgsql;

CREATE TRIGGER dummy_function_trigger
BEFORE UPDATE OF value ON citus_local_table
FOR EACH ROW EXECUTE FUNCTION dummy_function();

-- Show that we can sync metadata successfully. That means, we create
-- the function that trigger needs in mx workers too.
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);

CREATE EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table DEPENDS ON EXTENSION seg;
ALTER TRIGGER dummy_function_trigger ON citus_local_table RENAME TO renamed_trigger;
ALTER TABLE citus_local_table DISABLE TRIGGER ALL;
-- show that update trigger mx relation are depending on seg, renamed and disabled.
-- both workers should should print 1.
SELECT run_command_on_workers(
$$
SELECT COUNT(*) FROM pg_depend, pg_trigger, pg_extension
WHERE pg_trigger.tgrelid='citus_local_tables_mx.citus_local_table'::regclass AND
      pg_trigger.tgname='renamed_trigger' AND
      pg_trigger.tgenabled='D' AND
      pg_depend.classid='pg_trigger'::regclass AND
      pg_depend.deptype='x' AND
      pg_trigger.oid=pg_depend.objid AND
      pg_extension.extname='seg'
$$);

CREATE FUNCTION another_dummy_function() RETURNS trigger AS $another_dummy_function$
BEGIN
    RAISE EXCEPTION 'another trigger that throws another exception';
END;
$another_dummy_function$ LANGUAGE plpgsql;
-- Show that we can create the trigger successfully. That means, we create
-- the function that trigger needs in mx worker too when processing CREATE
-- TRIGGER commands.
CREATE TRIGGER another_dummy_function_trigger
AFTER TRUNCATE ON citus_local_table
FOR EACH STATEMENT EXECUTE FUNCTION another_dummy_function();

-- create some test tables before next three sections
-- and define some foreign keys between them

CREATE TABLE citus_local_table_1(l1 int);
SELECT create_citus_local_table('citus_local_table_1');
CREATE TABLE reference_table_1(r1 int primary key);
SELECT create_reference_table('reference_table_1');

ALTER TABLE citus_local_table_1 ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table_1(r1) ON DELETE CASCADE;

CREATE TABLE citus_local_table_2(l1 int primary key);
SELECT create_citus_local_table('citus_local_table_2');
CREATE TABLE reference_table_2(r1 int);
SELECT create_reference_table('reference_table_2');

ALTER TABLE reference_table_2 ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table_2(l1) ON DELETE RESTRICT;

CREATE TABLE citus_local_table_3(l1 int);
SELECT create_citus_local_table('citus_local_table_3');
CREATE TABLE citus_local_table_4(l1 int primary key);
SELECT create_citus_local_table('citus_local_table_4');

ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_local_to_local FOREIGN KEY(l1) REFERENCES citus_local_table_4(l1) ON UPDATE SET NULL;

-- and switch to worker1
\c - - - :worker_1_port
SET search_path TO citus_local_tables_mx;

-----------------------------------------------------------
-- foreign key from citus local table to reference table --
-----------------------------------------------------------

-- show that on delete cascade works
INSERT INTO reference_table_1 VALUES (11);
INSERT INTO citus_local_table_1 VALUES (11);
DELETE FROM reference_table_1 WHERE r1=11;
-- should print 0 rows
SELECT * FROM citus_local_table_1 ORDER BY l1;

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO citus_local_table_1 VALUES (2);

-- below should work
INSERT INTO reference_table_1 VALUES (2);
INSERT INTO citus_local_table_1 VALUES (2);

-----------------------------------------------------------
-- foreign key from reference table to citus local table --
-----------------------------------------------------------

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO reference_table_2 VALUES (4);

-- below should work
INSERT INTO citus_local_table_2 VALUES (4);
INSERT INTO reference_table_2 VALUES (4);

-------------------------------------------------------------
-- foreign key from citus local table to citus local table --
-------------------------------------------------------------

-- show that we are checking for foreign key constraint, below should fail
INSERT INTO citus_local_table_3 VALUES (3);

-- below shoud work
INSERT INTO citus_local_table_4 VALUES (3);
INSERT INTO citus_local_table_3 VALUES (3);

UPDATE citus_local_table_4 SET l1=6 WHERE l1=3;

-- show that it prints only one row with l1=null due to ON UPDATE SET NULL
SELECT * FROM citus_local_table_3;

-- finally show that we do not allow defining foreign key in mx nodes
ALTER TABLE citus_local_table_3 ADD CONSTRAINT fkey_local_to_local_2 FOREIGN KEY(l1) REFERENCES citus_local_table_4(l1);

\c - - - :master_port
SET search_path TO citus_local_tables_mx;

SELECT master_remove_distributed_table_metadata_from_workers('citus_local_table_4'::regclass::oid, 'citus_local_tables_mx', 'citus_local_table_4');

-- both workers should print 0 as master_remove_distributed_table_metadata_from_workers
-- drops the table as well
SELECT run_command_on_workers(
$$
SELECT count(*) FROM pg_catalog.pg_tables WHERE tablename='citus_local_table_4'
$$);

-- cleanup at exit
DROP SCHEMA citus_local_tables_mx CASCADE;
