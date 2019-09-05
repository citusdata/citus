SET citus.next_shard_id TO 20010000;

CREATE SCHEMA type_tests;
CREATE SCHEMA type_tests2; -- to test creation in a specific schema and moving to schema
SET search_path TO type_tests;
SET citus.shard_count TO 4;

-- single statement transactions with a simple type used in a table
CREATE TYPE tc1 AS (a int, b int);
CREATE TABLE t1 (a int PRIMARY KEY, b tc1);
SELECT create_distributed_table('t1','a');
INSERT INTO t1 VALUES (1, (2,3)::tc1);
SELECT * FROM t1;
ALTER TYPE tc1 RENAME TO tc1_newname;
INSERT INTO t1 VALUES (3, (4,5)::tc1_newname); -- insert with a cast would fail if the rename didn't propagate
ALTER TYPE tc1_newname SET SCHEMA type_tests2;
INSERT INTO t1 VALUES (6, (7,8)::type_tests2.tc1_newname); -- insert with a cast would fail if the rename didn't propagate

-- single statement transactions with a an enum used in a table
CREATE TYPE te1 AS ENUM ('one', 'two', 'three');
CREATE TABLE t2 (a int PRIMARY KEY, b te1);
SELECT create_distributed_table('t2','a');
INSERT INTO t2 VALUES (1, 'two');
SELECT * FROM t2;

-- rename enum, subsequent operations on the type would fail if the rename was not propagated
ALTER TYPE te1 RENAME TO te1_newname;

-- add an extra value to the enum and use in table
ALTER TYPE te1_newname ADD VALUE 'four';
UPDATE t2 SET b = 'four';
SELECT * FROM t2;

-- change the schema of the type and use the new fully qualified name in an insert
ALTER TYPE te1_newname SET SCHEMA type_tests2;
INSERT INTO t2 VALUES (3, 'three'::type_tests2.te1_newname);

-- transaction block with simple type
BEGIN;
CREATE TYPE tc2 AS (a int, b int);
CREATE TABLE t3 (a int PRIMARY KEY, b tc2);
SELECT create_distributed_table('t3','a');
INSERT INTO t3 VALUES (4, (5,6)::tc2);
SELECT * FROM t3;
COMMIT;

-- transaction block with simple type
BEGIN;
CREATE TYPE te2 AS ENUM ('yes', 'no');
CREATE TABLE t4 (a int PRIMARY KEY, b te2);
SELECT create_distributed_table('t4','a');
INSERT INTO t4 VALUES (1, 'yes');
SELECT * FROM t4;
-- ALTER TYPE ... ADD VALUE does not work in transactions
COMMIT;

-- test some combination of types without ddl propagation, this will prevent the workers
-- from having those types created. They are created just-in-time on table distribution
SET citus.enable_ddl_propagation TO off;
CREATE TYPE tc3 AS (a int, b int);
CREATE TYPE tc4 AS (a int, b tc3[]);
CREATE TYPE tc5 AS (a int, b tc4);
CREATE TYPE te3 AS ENUM ('a','b');
RESET citus.enable_ddl_propagation;

CREATE TABLE t5 (a int PRIMARY KEY, b tc5[], c te3);
SELECT create_distributed_table('t5','a');

-- test adding a column to a table of a non-distributed type
SET citus.enable_ddl_propagation TO off;
CREATE TYPE te4 AS ENUM ('c','d');
CREATE TYPE tc6 AS (a int, b int);
RESET citus.enable_ddl_propagation;

-- types need to be fully qualified because of the search_path which is not supported by ALTER TYPE ... ADD COLUMN
ALTER TABLE t5 ADD COLUMN d type_tests.te4;
ALTER TABLE t5 ADD COLUMN e type_tests.tc6;

-- last two values are only there if above commands succeeded
INSERT INTO t5 VALUES (1, NULL, 'a', 'd', (1,2)::tc6);

-- deleting the enum cascade will remove the type from the table and the workers
DROP TYPE te3 CASCADE;

-- DELETE multiple types at once
DROP TYPE tc3, tc4, tc5 CASCADE;

-- clear objects
SET client_min_messages TO fatal; -- suppress cascading objects dropping
DROP SCHEMA type_tests CASCADE;
DROP SCHEMA type_tests2 CASCADE;