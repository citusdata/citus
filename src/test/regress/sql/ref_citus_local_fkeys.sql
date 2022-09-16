\set VERBOSITY terse

SET citus.next_shard_id TO 1506000;
SET citus.shard_replication_factor TO 1;
SET citus.enable_local_execution TO ON;
SET citus.log_local_commands TO ON;

CREATE SCHEMA ref_citus_local_fkeys;
SET search_path TO ref_citus_local_fkeys;

-- ensure that coordinator is added to pg_dist_node
SET client_min_messages to ERROR;
SELECT 1 FROM master_add_node('localhost', :master_port, groupId => 0);
RESET client_min_messages;

-- create test tables
CREATE TABLE citus_local_table(l1 int);
SELECT citus_add_local_table_to_metadata('citus_local_table');
CREATE TABLE reference_table(r1 int primary key);
SELECT create_reference_table('reference_table');

-----------------------------------------------------------
-- foreign key from citus local table to reference table --
-----------------------------------------------------------

-- we support ON DELETE CASCADE behaviour in "ALTER TABLE ADD fkey citus_local_table (to reference_table) commands
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE CASCADE;

-- show that on delete cascade works
INSERT INTO reference_table VALUES (11);
INSERT INTO citus_local_table VALUES (11);
DELETE FROM reference_table WHERE r1=11;
-- should print 0 rows
SELECT * FROM citus_local_table ORDER BY l1;

-- show that we support drop constraint
ALTER TABLE citus_local_table DROP CONSTRAINT fkey_local_to_ref;

-- we support ON UPDATE CASCADE behaviour in "ALTER TABLE ADD fkey citus_local_table (to reference table)" commands
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE CASCADE;

-- show that on update cascade works
INSERT INTO reference_table VALUES (12);
INSERT INTO citus_local_table VALUES (12);
UPDATE reference_table SET r1=13 WHERE r1=12;
-- should print a row with 13
SELECT * FROM citus_local_table ORDER BY l1;

-- drop constraint for next commands
ALTER TABLE citus_local_table DROP CONSTRAINT fkey_local_to_ref;

INSERT INTO citus_local_table VALUES (2);
-- show that we are checking for foreign key constraint while defining, below should fail
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

INSERT INTO reference_table VALUES (2);
-- this should work
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- show that we are checking for foreign key constraint after defining, this should fail
INSERT INTO citus_local_table VALUES (1);

INSERT INTO reference_table VALUES (1);
-- this should work
INSERT INTO citus_local_table VALUES (1);

-- drop and add constraint for next commands
ALTER TABLE citus_local_table DROP CONSTRAINT fkey_local_to_ref;
ALTER TABLE citus_local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- show that drop table without CASCADE errors out
DROP TABLE reference_table;
-- this should work
BEGIN;
  DROP TABLE reference_table CASCADE;
ROLLBACK;

-- drop tables finally
DROP TABLE citus_local_table, reference_table;

-----------------------------------------------------------
-- foreign key from reference table to citus local table --
-----------------------------------------------------------

-- first remove worker_2 to test the behavior when replicating a
-- reference table that has a foreign key to a citus local table
-- to a new node
SELECT 1 FROM master_remove_node('localhost', :worker_2_port);

-- create test tables
CREATE TABLE citus_local_table(l1 int primary key);
SELECT citus_add_local_table_to_metadata('citus_local_table');
CREATE TABLE reference_table(r1 int);
SELECT create_reference_table('reference_table');

INSERT INTO reference_table VALUES (3);
-- show that we are checking for foreign key constraint while defining, this should fail
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1);

-- we do not support CASCADE / SET NULL / SET DEFAULT behavior in "ALTER TABLE ADD fkey reference_table (to citus_local_table)" commands
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE CASCADE;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE SET NULL;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE SET DEFAULT;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON UPDATE CASCADE;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON UPDATE SET NULL;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON UPDATE SET DEFAULT;

INSERT INTO citus_local_table VALUES (3);
-- .. but we allow such foreign keys with RESTRICT behavior
BEGIN;
  ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE RESTRICT;
ROLLBACK;
-- .. and we allow such foreign keys with NO ACTION behavior
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE NO ACTION;

-- show that adding/dropping foreign keys from reference to citus local
-- tables works fine with remote execution too
SET citus.enable_local_execution TO OFF;
ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;
SELECT undistribute_table('citus_local_table');
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE NO ACTION;
SET citus.enable_local_execution TO ON;

ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES citus_local_table(l1) ON DELETE NO ACTION;

-- show that we are checking for foreign key constraint after defining, this should fail
INSERT INTO reference_table VALUES (4);

-- enable the worker_2 to show that we don't try to set up the foreign keys
-- between reference tables and citus local tables in worker_2 placements of
-- the reference tables
SELECT 1 FROM master_add_node('localhost', :worker_2_port);

-- show that we support drop constraint
BEGIN;
  ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;
ROLLBACK;

-- show that drop table errors as expected
DROP TABLE citus_local_table;
-- this should work
DROP TABLE citus_local_table CASCADE;

BEGIN;
  CREATE TABLE citus_local_table_1(a int, b int, unique (a,b));
  CREATE TABLE citus_local_table_2(a int, b int, unique (a,b));
  SELECT citus_add_local_table_to_metadata('citus_local_table_1');
  SELECT citus_add_local_table_to_metadata('citus_local_table_2');

  -- show that we properly handle multi column foreign keys
  ALTER TABLE citus_local_table_1 ADD CONSTRAINT multi_fkey FOREIGN KEY (a, b) REFERENCES citus_local_table_2(a, b);
COMMIT;

-- when local execution is disabled, citus local table cannot be created
BEGIN;
	SET citus.enable_local_execution TO false;
	CREATE TABLE referenced_table(id int primary key);
	SELECT create_reference_table('referenced_table');
	CREATE TABLE referencing_table(id int, ref_id int, FOREIGN KEY(ref_id) REFERENCES referenced_table(id) ON DELETE SET DEFAULT);
ROLLBACK;

CREATE TABLE set_on_default_test_referenced(
    col_1 int, col_2 int, col_3 int, col_4 int,
    unique (col_1, col_3)
);
SELECT create_reference_table('set_on_default_test_referenced');

-- from citus local to reference - 1
CREATE TABLE set_on_default_test_referencing(
    col_1 int, col_2 int, col_3 serial, col_4 int,
    FOREIGN KEY(col_1, col_3)
    REFERENCES set_on_default_test_referenced(col_1, col_3)
    ON UPDATE SET DEFAULT
);

CREATE TABLE set_on_default_test_referencing(
    col_1 serial, col_2 int, col_3 int, col_4 int
);

-- from citus local to reference - 2
ALTER TABLE set_on_default_test_referencing ADD CONSTRAINT fkey
FOREIGN KEY(col_1, col_3) REFERENCES set_on_default_test_referenced(col_1, col_3)
ON DELETE SET DEFAULT;

DROP TABLE set_on_default_test_referencing, set_on_default_test_referenced;

CREATE TABLE set_on_default_test_referenced(
    col_1 int, col_2 int, col_3 int, col_4 int,
    unique (col_1, col_3)
);
SELECT citus_add_local_table_to_metadata('set_on_default_test_referenced');

-- from citus local to citus local
CREATE TABLE set_on_default_test_referencing(
    col_1 int, col_2 int, col_3 serial, col_4 int,
    FOREIGN KEY(col_1, col_3)
    REFERENCES set_on_default_test_referenced(col_1, col_3)
    ON DELETE SET DEFAULT
);

-- cleanup at exit
DROP SCHEMA ref_citus_local_fkeys CASCADE;
