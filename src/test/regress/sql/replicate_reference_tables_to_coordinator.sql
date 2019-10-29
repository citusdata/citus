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

-- Interact with a local table
CREATE TABLE local_table(a int);
INSERT INTO local_table VALUES (30), (31);

EXPLAIN (costs off) SELECT * FROM numbers, local_table;
BEGIN;
INSERT INTO numbers SELECT * FROM local_table;
SELECT * FROM numbers, local_table WHERE numbers.a != local_table.a;
ROLLBACK;

-- clean-up
SET client_min_messages TO ERROR;
DROP TABLE squares, numbers, local_table;
DROP SCHEMA replicate_ref_to_coordinator CASCADE;
SET search_path TO DEFAULT;
RESET client_min_messages;
