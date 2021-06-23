--
-- MULTI_SEQUENCE_DEFAULT
--
-- Tests related to column defaults coming from a sequence
--

SET citus.next_shard_id TO 890000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
CREATE SCHEMA sequence_default;
SET search_path = sequence_default, public;


-- Cannot add a column involving DEFAULT nextval('..') because the table is not empty
CREATE SEQUENCE seq_0;
-- check sequence type & other things
\d seq_0
-- we can change the type of the sequence before using it in distributed tables
ALTER SEQUENCE seq_0 AS smallint;
\d seq_0
CREATE TABLE seq_test_0 (x int, y int);
SELECT create_distributed_table('seq_test_0','x');
INSERT INTO seq_test_0 SELECT 1, s FROM generate_series(1, 50) s;
ALTER TABLE seq_test_0 ADD COLUMN z int DEFAULT nextval('seq_0');
ALTER TABLE seq_test_0 ADD COLUMN z serial;
-- follow hint
ALTER TABLE seq_test_0 ADD COLUMN z int;
ALTER TABLE seq_test_0 ALTER COLUMN z SET DEFAULT nextval('seq_0');
SELECT * FROM seq_test_0 ORDER BY 1, 2 LIMIT 5;
\d seq_test_0
-- check sequence type -> since it was used in a distributed table
-- type has changed to the type of the column it was used
-- in this case column z is of type int
\d seq_0
-- cannot change the type of a sequence used in a distributed table
-- even if metadata is not synced to workers
ALTER SEQUENCE seq_0 AS bigint;
-- we can change other things like increment
-- if metadata is not synced to workers
ALTER SEQUENCE seq_0 INCREMENT BY 2;
\d seq_0



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
INSERT INTO sequence_default.seq_test_4 VALUES (1,2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
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
INSERT INTO sequence_default.seq_test_1 values (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
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
\d sequence_default.sequence_2
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- check rename is propagated properly when we use ALTER TABLE
ALTER TABLE sequence_2 RENAME TO seq_2;
-- check in the worker
\c - - - :worker_1_port
\d sequence_default.seq_2
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- check rename with another schema
-- we notice that schema is also propagated as one of the sequence's dependencies
CREATE SCHEMA sequence_default_0;
CREATE SEQUENCE sequence_default_0.seq_3;
CREATE TABLE seq_test_3 (x int, y bigint DEFAULT nextval('sequence_default_0.seq_3'));
SELECT create_distributed_table('seq_test_3', 'x');
ALTER SEQUENCE sequence_default_0.seq_3 RENAME TO sequence_3;
-- check in the worker
\c - - - :worker_1_port
\d sequence_default_0.sequence_3
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
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
INSERT INTO sequence_default.seq_test_5 VALUES (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- apply workaround
SELECT run_command_on_workers('DROP SCHEMA sequence_default_1 CASCADE');
-- now the sequence is gone from the worker as well
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_5 VALUES (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
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
\d sequence_default.seq_7
\d sequence_default.seq_7_par
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- Check that various ALTER SEQUENCE commands
-- are not allowed for a distributed sequence for now
CREATE SEQUENCE seq_8;
CREATE SCHEMA sequence_default_8;
-- can change schema in a sequence not yet distributed
ALTER SEQUENCE seq_8 SET SCHEMA sequence_default_8;
ALTER SEQUENCE sequence_default_8.seq_8 SET SCHEMA sequence_default;
CREATE TABLE seq_test_8 (x int, y int DEFAULT nextval('seq_8'), z bigserial);
SELECT create_distributed_table('seq_test_8', 'x');
-- cannot change sequence specifications
ALTER SEQUENCE seq_8 AS bigint;
ALTER SEQUENCE seq_8 INCREMENT BY 2;
ALTER SEQUENCE seq_8 MINVALUE 5 MAXVALUE 5000;
ALTER SEQUENCE seq_8 START WITH 6;
ALTER SEQUENCE seq_8 RESTART WITH 6;
ALTER SEQUENCE seq_8 NO CYCLE;
ALTER SEQUENCE seq_8 OWNED BY seq_test_7;
ALTER SEQUENCE seq_test_8_z_seq AS smallint;
ALTER SEQUENCE seq_test_8_z_seq INCREMENT BY 2;
ALTER SEQUENCE seq_test_8_z_seq MINVALUE 5 MAXVALUE 5000;
ALTER SEQUENCE seq_test_8_z_seq START WITH 6;
ALTER SEQUENCE seq_test_8_z_seq RESTART WITH 6;
ALTER SEQUENCE seq_test_8_z_seq NO CYCLE;
ALTER SEQUENCE seq_test_8_z_seq OWNED BY seq_test_7;
-- can change schema in a distributed sequence
-- sequence_default_8 will be created in workers as part of dependencies
ALTER SEQUENCE seq_8 SET SCHEMA sequence_default_8;
\c - - - :worker_1_port
\d sequence_default_8.seq_8
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- check we can change the schema when we use ALTER TABLE
ALTER TABLE sequence_default_8.seq_8 SET SCHEMA sequence_default;
\c - - - :worker_1_port
\d sequence_default.seq_8
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
DROP SCHEMA sequence_default_8;
SELECT run_command_on_workers('DROP SCHEMA IF EXISTS sequence_default_8 CASCADE');


-- cannot use more than one sequence in a column default
CREATE SEQUENCE seq_9;
CREATE SEQUENCE seq_10;
CREATE TABLE seq_test_9 (x int, y int DEFAULT nextval('seq_9') - nextval('seq_10'));
SELECT create_distributed_table('seq_test_9', 'x');
ALTER TABLE seq_test_9 ALTER COLUMN y SET DEFAULT nextval('seq_9');
SELECT create_distributed_table('seq_test_9', 'x');


-- we can change the owner role of a sequence
CREATE ROLE seq_role_0;
CREATE ROLE seq_role_1;
ALTER SEQUENCE seq_10 OWNER TO seq_role_0;
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
SELECT run_command_on_workers('CREATE ROLE seq_role_0');
SELECT run_command_on_workers('CREATE ROLE seq_role_1');
ALTER TABLE seq_test_9 ALTER COLUMN y SET DEFAULT nextval('seq_10');
ALTER SEQUENCE seq_10 OWNER TO seq_role_1;
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- check we can change the owner role of a sequence when we use ALTER TABLE
ALTER TABLE seq_10 OWNER TO seq_role_0;
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
\c - - - :worker_1_port
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
DROP SEQUENCE seq_10 CASCADE;
DROP ROLE seq_role_0, seq_role_1;
SELECT run_command_on_workers('DROP ROLE IF EXISTS seq_role_0, seq_role_1');


-- Check some cases when default is defined by
-- DEFAULT nextval('seq_name'::text) (not by DEFAULT nextval('seq_name'))
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
CREATE SEQUENCE seq_11;
CREATE TABLE seq_test_10 (col0 int, col1 int DEFAULT nextval('seq_11'::text));
SELECT create_reference_table('seq_test_10');
INSERT INTO seq_test_10 VALUES (0);
CREATE TABLE seq_test_11 (col0 int, col1 bigint DEFAULT nextval('seq_11'::text));
-- works but doesn't create seq_11 in the workers
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
-- works because there is no dependency created between seq_11 and seq_test_10
SELECT create_distributed_table('seq_test_11', 'col1');
-- insertion from workers fails
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_10 VALUES (1);
\c - - - :master_port


-- clean up
DROP TABLE sequence_default.seq_test_7_par;
DROP SCHEMA sequence_default CASCADE;
SELECT run_command_on_workers('DROP SCHEMA IF EXISTS sequence_default CASCADE');
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
SET search_path TO public;
