--
-- REPLICATE_REF_TABLES_ON_COORDINATOR
--

CREATE SCHEMA replicate_ref_to_coordinator;
SET search_path TO 'replicate_ref_to_coordinator';
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SET citus.next_shard_id TO 8000000;
SET citus.next_placement_id TO 8000000;

--- enable logging to see which tasks are executed locally
SET client_min_messages TO LOG;
SET citus.log_local_commands TO ON;

CREATE TABLE squares(a int, b int);
SELECT create_reference_table('squares');
INSERT INTO squares SELECT i, i * i FROM generate_series(1, 10) i;

-- should be executed locally
SELECT count(*) FROM squares;

-- create a second reference table
CREATE TABLE numbers(a int);
SELECT create_reference_table('numbers');
INSERT INTO numbers VALUES (20), (21);

-- INSERT ... SELECT between reference tables
BEGIN;
EXPLAIN INSERT INTO squares SELECT a, a*a FROM numbers;
INSERT INTO squares SELECT a, a*a FROM numbers;
SELECT * FROM squares WHERE a >= 20 ORDER BY a;
ROLLBACK;

BEGIN;
EXPLAIN INSERT INTO numbers SELECT a FROM squares WHERE a < 3;
INSERT INTO numbers SELECT a FROM squares WHERE a < 3;
SELECT * FROM numbers ORDER BY a;
ROLLBACK;

-- Make sure we hide shard tables ...
SELECT citus_table_is_visible('numbers_8000001'::regclass::oid);

-- Join between reference tables and local tables
CREATE TABLE local_table(a int);
INSERT INTO local_table VALUES (2), (4), (7), (20);

EXPLAIN SELECT local_table.a, numbers.a FROM local_table NATURAL JOIN numbers;
SELECT local_table.a, numbers.a FROM local_table NATURAL JOIN numbers ORDER BY 1;

-- error if in transaction block
BEGIN;
SELECT local_table.a, numbers.a FROM local_table NATURAL JOIN numbers ORDER BY 1;
ROLLBACK;

-- error if in a transaction block even if reference table is not in search path
CREATE SCHEMA s1;
CREATE TABLE s1.ref(a int);
SELECT create_reference_table('s1.ref');

BEGIN;
SELECT local_table.a, r.a FROM local_table NATURAL JOIN s1.ref r ORDER BY 1;
ROLLBACK;

DROP SCHEMA s1 CASCADE;

-- shouldn't plan locally if modifications happen in CTEs, ...
WITH ins AS (INSERT INTO numbers VALUES (1) RETURNING *) SELECT * FROM numbers, local_table;
WITH t AS (SELECT *, random() x FROM numbers FOR UPDATE) SELECT * FROM numbers, local_table
  WHERE EXISTS (SELECT * FROM t WHERE t.x = numbers.a);

-- but this should be fine
WITH t AS (SELECT *, random() x FROM numbers) SELECT * FROM numbers, local_table
  WHERE EXISTS (SELECT * FROM t WHERE t.x = numbers.a);

-- shouldn't plan locally even if distributed table is in CTE or subquery
CREATE TABLE dist(a int);
SELECT create_distributed_table('dist', 'a');
WITH t AS (SELECT *, random() x FROM dist) SELECT * FROM numbers, local_table
  WHERE EXISTS (SELECT * FROM t WHERE t.x = numbers.a);

 -- error if FOR UPDATE/FOR SHARE
 SELECT local_table.a, numbers.a FROM local_table NATURAL JOIN numbers FOR SHARE;
 SELECT local_table.a, numbers.a FROM local_table NATURAL JOIN numbers FOR UPDATE;

-- clean-up
SET client_min_messages TO ERROR;
DROP SCHEMA replicate_ref_to_coordinator CASCADE;

-- Make sure the shard was dropped
 SELECT 'numbers_8000001'::regclass::oid;

SET search_path TO DEFAULT;
RESET client_min_messages;
