--
-- MULTI_SEQUENCE_DEFAULT
--
-- Tests related to column defaults coming from a sequence
--

SET citus.next_shard_id TO 890000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;


-- Cannot add a column involving DEFAULT nextval('..') because the table is not empty
CREATE SEQUENCE seq_0;
CREATE TABLE seq_test_0 (x int, y int);
SELECT create_distributed_table('seq_test_0','x');
INSERT INTO seq_test_0 SELECT 1, s FROM generate_series(1, 50) s;
ALTER TABLE seq_test_0 ADD COLUMN z int DEFAULT nextval('seq_0');
-- follow hint
ALTER TABLE seq_test_0 ADD COLUMN z int;
ALTER TABLE seq_test_0 ALTER COLUMN z SET DEFAULT nextval('seq_0');
SELECT * FROM seq_test_0 ORDER BY 1, 2 LIMIT 5;
\d seq_test_0


-- check that we can add serial pseudo-type columns
-- when metadata is not yet synced to workers
TRUNCATE seq_test_0;
ALTER TABLE seq_test_0 ADD COLUMN w00 smallserial;
ALTER TABLE seq_test_0 ADD COLUMN w01 serial2;
ALTER TABLE seq_test_0 ADD COLUMN w10 serial;
ALTER TABLE seq_test_0 ADD COLUMN w11 serial4;
ALTER TABLE seq_test_0 ADD COLUMN w20 bigserial;
ALTER TABLE seq_test_0 ADD COLUMN w21 serial8;

-- check alter column type precaution
ALTER TABLE seq_test_0 ALTER COLUMN z TYPE bigint;
ALTER TABLE seq_test_0 ALTER COLUMN z TYPE smallint;


-- MX tests

-- check that there's not problem with group ID cache
CREATE TABLE seq_test_4 (x int, y int);
SELECT create_distributed_table('seq_test_4','x');
CREATE SEQUENCE seq_4;
ALTER TABLE seq_test_4 ADD COLUMN a int DEFAULT nextval('seq_4');
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
DROP SEQUENCE seq_4 CASCADE;
TRUNCATE seq_test_4;
CREATE SEQUENCE seq_4;
ALTER TABLE seq_test_4 ADD COLUMN b int DEFAULT nextval('seq_4');
-- on worker it should generate high sequence number
\c - - - :worker_1_port
INSERT INTO seq_test_4 VALUES (1,2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check sequence type consistency in all nodes
CREATE SEQUENCE seq_1;
-- type is bigint by default
\d seq_1
CREATE TABLE seq_test_1 (x int, y int);
SELECT create_distributed_table('seq_test_1','x');
ALTER TABLE seq_test_1 ADD COLUMN z int DEFAULT nextval('seq_1');
-- type is changed to int
\d seq_1
-- check insertion is within int bounds in the worker
\c - - - :worker_1_port
INSERT INTO seq_test_1 values (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check that we cannot add serial pseudo-type columns
-- when metadata is synced to workers
ALTER TABLE seq_test_1 ADD COLUMN w bigserial;


-- check for sequence type clashes
CREATE SEQUENCE seq_2;
CREATE TABLE seq_test_2 (x int, y bigint DEFAULT nextval('seq_2'));
-- should work
SELECT create_distributed_table('seq_test_2','x');
DROP TABLE seq_test_2;
CREATE TABLE seq_test_2 (x int, y int DEFAULT nextval('seq_2'));
-- should work
SELECT create_distributed_table('seq_test_2','x');
CREATE TABLE seq_test_2_0(x int, y smallint DEFAULT nextval('seq_2'));
-- shouldn't work
SELECT create_distributed_table('seq_test_2_0','x');
DROP TABLE seq_test_2;
DROP TABLE seq_test_2_0;
-- should work
CREATE TABLE seq_test_2 (x int, y bigint DEFAULT nextval('seq_2'));
SELECT create_distributed_table('seq_test_2','x');
DROP TABLE seq_test_2;
CREATE TABLE seq_test_2 (x int, y int DEFAULT nextval('seq_2'), z bigint DEFAULT nextval('seq_2'));
-- shouldn't work
SELECT create_distributed_table('seq_test_2','x');


-- check rename is propagated properly
ALTER SEQUENCE seq_2 RENAME TO sequence_2;
-- check in the worker
\c - - - :worker_1_port
\d sequence_2
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- check rename with another schema
-- we notice that schema is also propagated as one of the sequence's dependencies
CREATE SCHEMA sequence_default_0;
SET search_path TO 	public, sequence_default_0;
CREATE SEQUENCE sequence_default_0.seq_3;
CREATE TABLE seq_test_3 (x int, y bigint DEFAULT nextval('seq_3'));
SELECT create_distributed_table('seq_test_3', 'x');
ALTER SEQUENCE sequence_default_0.seq_3 RENAME TO sequence_3;
-- check in the worker
\c - - - :worker_1_port
\d sequence_default_0.sequence_3
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
DROP SEQUENCE sequence_default_0.sequence_3 CASCADE;
DROP SCHEMA sequence_default_0;


-- DROP SCHEMA problem: expected since we don't propagate DROP SCHEMA
CREATE TABLE seq_test_5 (x int, y int);
SELECT create_distributed_table('seq_test_5','x');
CREATE SCHEMA sequence_default_1;
CREATE SEQUENCE sequence_default_1.seq_5;
ALTER TABLE seq_test_5 ADD COLUMN a int DEFAULT nextval('sequence_default_1.seq_5');
DROP SCHEMA sequence_default_1 CASCADE;
-- sequence is gone from coordinator
INSERT INTO seq_test_5 VALUES (1, 2) RETURNING *;
-- but is still present on worker
\c - - - :worker_1_port
INSERT INTO seq_test_5 VALUES (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- apply workaround
SELECT run_command_on_workers('DROP SCHEMA sequence_default_1 CASCADE');
-- now the sequence is gone from the worker as well
\c - - - :worker_1_port
INSERT INTO seq_test_5 VALUES (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check some more complex cases
CREATE SEQUENCE seq_6;
CREATE TABLE seq_test_6 (x int, t timestamptz DEFAULT now(), s int DEFAULT nextval('seq_6'), m int) PARTITION BY RANGE (t);
SELECT create_distributed_table('seq_test_6','x');
-- shouldn't work since x is the partition column
ALTER TABLE seq_test_6 ALTER COLUMN x SET DEFAULT nextval('seq_6');
-- should work since both s and m have int type
ALTER TABLE seq_test_6 ALTER COLUMN m SET DEFAULT nextval('seq_6');


-- It is possible for a partition to have a different DEFAULT than its parent
CREATE SEQUENCE seq_7;
CREATE TABLE seq_test_7 (x text, s bigint DEFAULT nextval('seq_7'), t timestamptz DEFAULT now()) PARTITION BY RANGE (t);
SELECT create_distributed_table('seq_test_7','x');
CREATE SEQUENCE seq_7_par;
CREATE TABLE seq_test_7_par (x text, s bigint DEFAULT nextval('seq_7_par'), t timestamptz DEFAULT now());
ALTER TABLE seq_test_7 ATTACH PARTITION seq_test_7_par FOR VALUES FROM ('2021-05-31') TO ('2021-06-01');
-- check that both sequences are in worker
\c - - - :worker_1_port
\d seq_7
\d seq_7_par
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- Check that various ALTER SEQUENCE commands
-- are not allowed for a distributed sequence for now
CREATE SEQUENCE seq_8;
CREATE TABLE seq_test_8 (x int, y int DEFAULT nextval('seq_8'));
SELECT create_distributed_table('seq_test_8', 'x');
ALTER SEQUENCE seq_8 AS bigint;
ALTER SEQUENCE seq_8 INCREMENT BY 2;
ALTER SEQUENCE seq_8 MINVALUE 5 MAXVALUE 5000;
ALTER SEQUENCE seq_8 START WITH 6;
ALTER SEQUENCE seq_8 RESTART WITH 6;
ALTER SEQUENCE seq_8 NO CYCLE;
ALTER SEQUENCE seq_8 OWNED BY seq_test_7;
CREATE SCHEMA sequence_default_8;
ALTER SEQUENCE seq_8 SET SCHEMA sequence_default_8;
DROP SCHEMA sequence_default_8;


-- clean up
DROP TABLE seq_test_0, seq_test_1, seq_test_2, seq_test_3, seq_test_4, seq_test_5, seq_test_6, seq_test_7, seq_test_8;
DROP SEQUENCE seq_0, seq_1, sequence_2, seq_4, seq_6, seq_7, seq_7_par, seq_8;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
