CREATE SCHEMA fkey_reference_local_table;
SET search_path TO 'fkey_reference_local_table';

-- create test tables

CREATE TABLE local_table(l1 int);
CREATE TABLE reference_table(r1 int primary key);
SELECT create_reference_table('reference_table');

-- foreign key from local table to reference table --

-- this should fail
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- we do not support ALTER TABLE ADD CONSTRAINT fkey between local & reference table
-- within a transaction block, below block should fail
BEGIN;
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);
COMMIT;

-- replicate reference table to coordinator
SELECT master_add_node('localhost', :master_port, groupId => 0);

-- we support ON DELETE CASCADE behaviour in "ALTER TABLE ADD fkey local_table (to reference_table) commands
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON DELETE CASCADE;

-- show that on delete cascade works
INSERT INTO reference_table VALUES (11);
INSERT INTO local_table VALUES (11);
DELETE FROM reference_table WHERE r1=11;
SELECT * FROM local_table ORDER BY l1;

-- show that we support drop constraint
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

-- we support ON UPDATE CASCADE behaviour in "ALTER TABLE ADD fkey local_table (to reference table)" commands
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1) ON UPDATE CASCADE;

-- show that on delete cascade works
INSERT INTO reference_table VALUES (12);
INSERT INTO local_table VALUES (12);
UPDATE reference_table SET r1=13 WHERE r1=12;
SELECT * FROM local_table ORDER BY l1;

-- drop constraint for next commands
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

-- show that we are checking for foreign key constraint while defining

INSERT INTO local_table VALUES (2);

-- this should fail
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

INSERT INTO reference_table VALUES (2);

-- this should work
ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- show that we are checking for foreign key constraint after defining

-- this should fail
INSERT INTO local_table VALUES (1);

INSERT INTO reference_table VALUES (1);

-- this should work
INSERT INTO local_table VALUES (1);

-- we do not support ALTER TABLE DROP CONSTRAINT fkey between local & reference table
-- within a transaction block, below block should fail
BEGIN;
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;
COMMIT;

-- show that we do not allow removing coordinator when we have a fkey constraint
-- between a coordinator local table and a reference table
SELECT master_remove_node('localhost', :master_port);

-- drop and add constraint for next commands
ALTER TABLE local_table DROP CONSTRAINT fkey_local_to_ref;

ALTER TABLE local_table ADD CONSTRAINT fkey_local_to_ref FOREIGN KEY(l1) REFERENCES reference_table(r1);

-- show that drop table without CASCADE does not error as we already append CASCADE
DROP TABLE reference_table;

-- drop local_table finally
DROP TABLE local_table;

-- TODO: drop them together after fixing the bug

-- create test tables

CREATE TABLE local_table(l1 int primary key);
CREATE TABLE reference_table(r1 int);
SELECT create_reference_table('reference_table');

-- remove master node from pg_dist_node
SELECT master_remove_node('localhost', :master_port);

-- foreign key from reference table to local table --

-- this should fail
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- we do not support ALTER TABLE ADD CONSTRAINT fkey between local & reference table
-- within a transaction block, below block should fail
BEGIN;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);
COMMIT;

-- replicate reference table to coordinator

SELECT master_add_node('localhost', :master_port, groupId => 0);

-- show that we are checking for foreign key constraint while defining

INSERT INTO reference_table VALUES (3);

-- this should fail
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

INSERT INTO local_table VALUES (3);

-- this should work
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- show that we are checking for foreign key constraint after defining

-- this should fail
INSERT INTO reference_table VALUES (4);

INSERT INTO local_table VALUES (4);

-- we do not support ON DELETE/UPDATE CASCADE behaviour in "ALTER TABLE ADD fkey reference_table (to local_table)" commands
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1) ON DELETE CASCADE;
ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1) ON UPDATE CASCADE;

-- this should work
INSERT INTO reference_table VALUES (4);

-- we do not support ALTER TABLE DROP CONSTRAINT fkey between local & reference table
-- within a transaction block, below block should fail
BEGIN;
ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;
COMMIt;

-- show that we do not allow removing coordinator when we have a fkey constraint
-- between a coordinator local table and a reference table
SELECT master_remove_node('localhost', :master_port);

-- show that we support drop constraint
ALTER TABLE reference_table DROP CONSTRAINT fkey_ref_to_local;

ALTER TABLE reference_table ADD CONSTRAINT fkey_ref_to_local FOREIGN KEY(r1) REFERENCES local_table(l1);

-- show that drop table errors as expected
DROP TABLE local_table;

-- this should work
DROP TABLE local_table CASCADE;

-- drop reference_table finally
DROP TABLE reference_table;

-- TODO: drop them together after fixing the bug

-- TODO: test schema / table name escape
-- TODO: test create table behaviour, also implement error out conditions
-- TODO: partitioned tables

-- TEARDOWN -- -- -- --

-- print table reference table content to make sure we successfully inserted/deleted values
SELECT * FROM reference_table ORDER BY r1;

-- remove master node from pg_dist_node
SELECT master_remove_node('localhost', :master_port);

-- print table contents to make sure we successfully inserted/deleted values
SELECT * FROM local_table ORDER BY l1;
SELECT * FROM reference_table ORDER BY r1;

DROP SCHEMA fkey_reference_local_table;
