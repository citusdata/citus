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


-- test both distributed and citus local tables
-- Cannot add a column involving DEFAULT nextval('..') because the table is not empty
CREATE SEQUENCE seq_0;
CREATE SEQUENCE seq_0_local_table;
-- check sequence type & other things
\d seq_0
\d seq_0_local_table
-- we can change the type of the sequence before using it in distributed tables
ALTER SEQUENCE seq_0 AS smallint;
\d seq_0
CREATE TABLE seq_test_0 (x int, y int);
CREATE TABLE seq_test_0_local_table (x int, y int);
SELECT create_distributed_table('seq_test_0','x');
SELECT citus_add_local_table_to_metadata('seq_test_0_local_table');
INSERT INTO seq_test_0 SELECT 1, s FROM generate_series(1, 50) s;
INSERT INTO seq_test_0_local_table SELECT 1, s FROM generate_series(1, 50) s;
ALTER TABLE seq_test_0 ADD COLUMN z int DEFAULT nextval('seq_0');
ALTER TABLE seq_test_0_local_table ADD COLUMN z int DEFAULT nextval('seq_0_local_table');
ALTER TABLE seq_test_0 ADD COLUMN z serial;
ALTER TABLE seq_test_0_local_table ADD COLUMN z serial;
-- follow hint
ALTER TABLE seq_test_0 ADD COLUMN z int;
ALTER TABLE seq_test_0 ALTER COLUMN z SET DEFAULT nextval('seq_0');
SELECT * FROM seq_test_0 ORDER BY 1, 2 LIMIT 5;
\d seq_test_0
ALTER TABLE seq_test_0_local_table ADD COLUMN z int;
ALTER TABLE seq_test_0_local_table ALTER COLUMN z SET DEFAULT nextval('seq_0_local_table');
SELECT * FROM seq_test_0_local_table ORDER BY 1, 2 LIMIT 5;
\d seq_test_0_local_table
-- check sequence type -> since it was used in a distributed table
-- type has changed to the type of the column it was used
-- in this case column z is of type int
\d seq_0
\d seq_0_local_table
-- cannot alter a sequence used in a distributed table
-- since the metadata is synced to workers
ALTER SEQUENCE seq_0 AS bigint;
ALTER SEQUENCE seq_0_local_table AS bigint;

-- check alter column type precaution
ALTER TABLE seq_test_0 ALTER COLUMN z TYPE bigint;
ALTER TABLE seq_test_0 ALTER COLUMN z TYPE smallint;

ALTER TABLE seq_test_0_local_table ALTER COLUMN z TYPE bigint;
ALTER TABLE seq_test_0_local_table ALTER COLUMN z TYPE smallint;


-- MX tests

-- check that there's not problem with group ID cache
CREATE TABLE seq_test_4 (x int, y int);
SELECT create_distributed_table('seq_test_4','x');
CREATE SEQUENCE seq_4;
ALTER TABLE seq_test_4 ADD COLUMN a bigint DEFAULT nextval('seq_4');
ALTER TABLE seq_test_4 ADD COLUMN IF NOT EXISTS a bigint DEFAULT nextval('seq_4');
DROP SEQUENCE seq_4 CASCADE;
TRUNCATE seq_test_4;
CREATE SEQUENCE seq_4;
ALTER TABLE seq_test_4 ADD COLUMN b bigint DEFAULT nextval('seq_4');
-- on worker it should generate high sequence number
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_4 VALUES (1,2) RETURNING *;

-- check that we have can't insert to tables from before metadata sync
-- seq_test_0 and seq_test_0_local_table have int and smallint column defaults
INSERT INTO sequence_default.seq_test_0 VALUES (1,2) RETURNING *;
INSERT INTO sequence_default.seq_test_0_local_table VALUES (1,2) RETURNING *;

\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check sequence type consistency in all nodes for distributed tables
CREATE SEQUENCE seq_1;
-- type is bigint by default
\d seq_1
CREATE TABLE seq_test_1 (x int, y int);
SELECT create_distributed_table('seq_test_1','x');
ALTER TABLE seq_test_1 ADD COLUMN z int DEFAULT nextval('seq_1');
-- type is changed to int
\d seq_1
-- check insertion doesn't work in the worker because type is int
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_1 values (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check sequence type consistency in all nodes for citus local tables
CREATE SEQUENCE seq_1_local_table;
-- type is bigint by default
\d seq_1_local_table
CREATE TABLE seq_test_1_local_table (x int, y int);
SELECT citus_add_local_table_to_metadata('seq_test_1_local_table');
ALTER TABLE seq_test_1_local_table ADD COLUMN z int DEFAULT nextval('seq_1_local_table');
-- type is changed to int
\d seq_1_local_table
-- check insertion doesn't work in the worker because type is int
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_1_local_table values (1, 2) RETURNING *;
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);


-- check that we cannot add serial pseudo-type columns
-- when metadata is synced to workers
ALTER TABLE seq_test_1 ADD COLUMN w bigserial;
ALTER TABLE seq_test_1_local_table ADD COLUMN w bigserial;


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
SELECT citus_add_local_table_to_metadata('seq_test_2_0');
DROP TABLE seq_test_2;
DROP TABLE seq_test_2_0;
-- should work
CREATE TABLE seq_test_2 (x int, y bigint DEFAULT nextval('seq_2'));
SELECT create_distributed_table('seq_test_2','x');
DROP TABLE seq_test_2;
CREATE TABLE seq_test_2 (x int, y int DEFAULT nextval('seq_2'), z bigint DEFAULT nextval('seq_2'));
-- shouldn't work
SELECT create_distributed_table('seq_test_2','x');
SELECT citus_add_local_table_to_metadata('seq_test_2');


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
-- Also check that various sequence options are passed on to the worker
-- correctly
CREATE SEQUENCE seq_8 AS integer INCREMENT BY 3 CACHE 10 CYCLE;
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
ALTER SEQUENCE seq_8 CACHE 5;
ALTER SEQUENCE seq_8 NO CYCLE;
ALTER SEQUENCE seq_8 OWNED BY seq_test_7;
ALTER SEQUENCE seq_test_8_z_seq AS smallint;
ALTER SEQUENCE seq_test_8_z_seq INCREMENT BY 2;
ALTER SEQUENCE seq_test_8_z_seq MINVALUE 5 MAXVALUE 5000;
ALTER SEQUENCE seq_test_8_z_seq START WITH 6;
ALTER SEQUENCE seq_test_8_z_seq RESTART WITH 6;
ALTER SEQUENCE seq_test_8_z_seq CACHE 5;
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
DROP SCHEMA sequence_default_8 CASCADE;


-- cannot use more than one sequence in a column default
CREATE SEQUENCE seq_9;
CREATE SEQUENCE seq_10;
CREATE TABLE seq_test_9 (x int, y int DEFAULT nextval('seq_9') - nextval('seq_10'));
SELECT create_distributed_table('seq_test_9', 'x');
SELECT citus_add_local_table_to_metadata('seq_test_9');
ALTER TABLE seq_test_9 ALTER COLUMN y SET DEFAULT nextval('seq_9');
SELECT create_distributed_table('seq_test_9', 'x');


-- we can change the owner role of a sequence
CREATE ROLE seq_role_0;
CREATE ROLE seq_role_1;
ALTER SEQUENCE seq_10 OWNER TO seq_role_0;
SELECT sequencename, sequenceowner FROM pg_sequences WHERE sequencename = 'seq_10' ORDER BY 1, 2;
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


-- Check some cases when default is defined by
-- DEFAULT nextval('seq_name'::text) (not by DEFAULT nextval('seq_name'))
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);
CREATE SEQUENCE seq_11;
CREATE TABLE seq_test_10 (col0 int, col1 int DEFAULT nextval('seq_11'::text));
SELECT create_reference_table('seq_test_10');
INSERT INTO seq_test_10 VALUES (0);
CREATE TABLE seq_test_11 (col0 int, col1 bigint DEFAULT nextval('seq_11'::text));
-- works but doesn't create seq_11 in the workers
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
-- works because there is no dependency created between seq_11 and seq_test_10
SELECT create_distributed_table('seq_test_11', 'col1');
-- insertion from workers fails
\c - - - :worker_1_port
INSERT INTO sequence_default.seq_test_10 VALUES (1);
\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT stop_metadata_sync_to_node('localhost', :worker_1_port);


-- Check worker_nextval and setval precautions for int and smallint column defaults
-- For details see issue #5126 and PR #5254
-- https://github.com/citusdata/citus/issues/5126
CREATE SEQUENCE seq_12;
CREATE SEQUENCE seq_13;
CREATE SEQUENCE seq_14;
CREATE TABLE seq_test_12(col0 text, col1 smallint DEFAULT nextval('seq_12'),
                         col2 int DEFAULT nextval('seq_13'),
                         col3 bigint DEFAULT nextval('seq_14'));
SELECT create_distributed_table('seq_test_12', 'col0');
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
INSERT INTO seq_test_12 VALUES ('hello0') RETURNING *;

\c - - - :worker_1_port
SET search_path = sequence_default, public;
-- we should see worker_nextval for int and smallint columns
SELECT table_name, column_name, data_type, column_default FROM information_schema.columns
WHERE table_name = 'seq_test_12' ORDER BY column_name;
-- insertion from worker should fail
INSERT INTO seq_test_12 VALUES ('hello1') RETURNING *;
-- nextval from worker should fail for int and smallint sequences
SELECT nextval('seq_12');
SELECT nextval('seq_13');
-- nextval from worker should work for bigint sequences
SELECT nextval('seq_14');

\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT start_metadata_sync_to_node('localhost', :worker_1_port);
TRUNCATE seq_test_12;
ALTER TABLE seq_test_12 DROP COLUMN col1;
ALTER TABLE seq_test_12 DROP COLUMN col2;
ALTER TABLE seq_test_12 DROP COLUMN col3;
DROP SEQUENCE seq_12, seq_13, seq_14;
CREATE SEQUENCE seq_12;
CREATE SEQUENCE seq_13;
CREATE SEQUENCE seq_14;
ALTER TABLE seq_test_12 ADD COLUMN col1 smallint DEFAULT nextval('seq_14');
ALTER TABLE seq_test_12 ADD COLUMN col2 int DEFAULT nextval('seq_13');
ALTER TABLE seq_test_12 ADD COLUMN col3 bigint DEFAULT nextval('seq_12');
ALTER TABLE seq_test_12 ADD COLUMN col4 smallint;
ALTER TABLE seq_test_12 ALTER COLUMN col4 SET DEFAULT nextval('seq_14');
ALTER TABLE seq_test_12 ADD COLUMN col5 int;
ALTER TABLE seq_test_12 ALTER COLUMN col5 SET DEFAULT nextval('seq_13');
ALTER TABLE seq_test_12 ADD COLUMN col6 bigint;
ALTER TABLE seq_test_12 ALTER COLUMN col6 SET DEFAULT nextval('seq_12');
INSERT INTO seq_test_12 VALUES ('hello1') RETURNING *;

\c - - - :worker_1_port
SET search_path = sequence_default, public;
-- we should see worker_nextval for int and smallint columns
SELECT table_name, column_name, data_type, column_default FROM information_schema.columns
WHERE table_name = 'seq_test_12' ORDER BY column_name;
-- insertion from worker should fail
INSERT INTO seq_test_12 VALUES ('hello2') RETURNING *;
-- nextval from worker should work for bigint sequences
SELECT nextval('seq_12');
-- nextval from worker should fail for int and smallint sequences
SELECT nextval('seq_13');
SELECT nextval('seq_14');

\c - - - :master_port
SET citus.shard_replication_factor TO 1;
SET search_path = sequence_default, public;
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);
SELECT undistribute_table('seq_test_12');
SELECT create_distributed_table('seq_test_12', 'col0');
INSERT INTO seq_test_12 VALUES ('hello2') RETURNING *;

\c - - - :worker_1_port
SET search_path = sequence_default, public;
-- we should see worker_nextval for int and smallint columns
SELECT table_name, column_name, data_type, column_default FROM information_schema.columns
WHERE table_name = 'seq_test_12' ORDER BY column_name;
-- insertion from worker should fail
INSERT INTO seq_test_12 VALUES ('hello2') RETURNING *;
-- nextval from worker should work for bigint sequences
SELECT nextval('seq_12');
-- nextval from worker should fail for int and smallint sequences
SELECT nextval('seq_13');
SELECT nextval('seq_14');

\c - - - :master_port

-- Show that sequence and its dependency schema will be propagated if a distributed
-- table with default column is added
CREATE SCHEMA test_schema_for_sequence_default_propagation;
CREATE SEQUENCE test_schema_for_sequence_default_propagation.seq_10;

-- Create distributed table with default column to propagate dependencies
CREATE TABLE test_seq_dist(a int, x BIGINT DEFAULT nextval('test_schema_for_sequence_default_propagation.seq_10'));
SELECT create_distributed_table('test_seq_dist', 'a');

-- Both sequence and dependency schema should be distributed
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object WHERE objid IN ('test_schema_for_sequence_default_propagation.seq_10'::regclass);
SELECT pg_identify_object_as_address(classid, objid, objsubid) from pg_catalog.pg_dist_object WHERE objid IN ('test_schema_for_sequence_default_propagation'::regnamespace);

-- Show that sequence can stay on the worker node if the transaction is
-- rollbacked after distributing the table
BEGIN;
CREATE SEQUENCE sequence_rollback;
CREATE TABLE sequence_rollback_table(id int, val_1 int default nextval('sequence_rollback'));
SELECT create_distributed_table('sequence_rollback_table', 'id');
ROLLBACK;

-- Show that there is a sequence on the worker with the sequence type int
\c - - - :worker_1_port
SELECT seqtypid::regtype, seqmax, seqmin FROM pg_sequence WHERE seqrelid::regclass::text = 'sequence_rollback';

\c - - - :master_port
-- Show that we can create a sequence with the same name and different data type
BEGIN;
CREATE SEQUENCE sequence_rollback;
CREATE TABLE sequence_rollback_table(id int, val_1 bigint default nextval('sequence_rollback'));
SELECT create_distributed_table('sequence_rollback_table', 'id');
ROLLBACK;

-- Show that existing sequence has been renamed and a new sequence with the same name
-- created for another type
\c - - - :worker_1_port
SELECT seqrelid::regclass, seqtypid::regtype, seqmax, seqmin FROM pg_sequence WHERE seqrelid::regclass::text in ('sequence_rollback', '"sequence_rollback(citus_backup_0)"') ORDER BY 1,2;

\c - - - :master_port

-- clean up
DROP SCHEMA test_schema_for_sequence_default_propagation CASCADE;
DROP TABLE test_seq_dist;
DROP TABLE sequence_default.seq_test_7_par;
SET client_min_messages TO error; -- suppress cascading objects dropping
DROP SCHEMA sequence_default CASCADE;
SET search_path TO public;
