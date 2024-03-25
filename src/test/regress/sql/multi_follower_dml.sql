\c - - - :master_port

SET citus.shard_replication_factor TO 2;
CREATE TABLE the_replicated_table (a int, b int, z bigserial);
SELECT create_distributed_table('the_replicated_table', 'a');

SET citus.shard_replication_factor TO 1;
CREATE TABLE the_table (a int, b int, z bigserial);
SELECT create_distributed_table('the_table', 'a');

SELECT 1 FROM master_add_node('localhost', :master_port, groupid => 0);

CREATE TABLE reference_table (a int, b int, z bigserial);
SELECT create_reference_table('reference_table');

CREATE TABLE citus_local_table (a int, b int, z bigserial);
SELECT citus_add_local_table_to_metadata('citus_local_table');

CREATE TABLE local (a int, b int);

\c - - - :follower_master_port

-- inserts normally do not work on a standby coordinator
INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);
INSERT INTO reference_table (a, b, z) VALUES (1, 2, 2);
INSERT INTO citus_local_table (a, b, z) VALUES (1, 2, 2);

-- We can allow DML on a writable standby coordinator.
-- Note that it doesn't help to enable writes for
--   (a) citus local tables
--   (b) coordinator replicated reference tables.
--   (c) reference tables or replication > 1 distributed tables
-- (a) and (b) is because the data is in the coordinator and will hit
-- read-only tranaction checks on Postgres
-- (c) is because citus uses 2PC, where a transaction record should
-- be inserted to pg_dist_node, which is not allowed
SET citus.writable_standby_coordinator TO on;

INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);
SELECT * FROM the_table;
INSERT INTO the_replicated_table (a, b, z) VALUES (1, 2, 2);
SELECT * FROM the_replicated_table;
INSERT INTO reference_table (a, b, z) VALUES (1, 2, 2);
SELECT * FROM reference_table;
INSERT INTO citus_local_table (a, b, z) VALUES (1, 2, 2);
SELECT * FROM citus_local_table;

UPDATE the_table SET z = 3 WHERE a = 1;
UPDATE the_replicated_table SET z = 3 WHERE a = 1;
UPDATE reference_table SET z = 3 WHERE a = 1;
UPDATE citus_local_table SET z = 3 WHERE a = 1;
SELECT * FROM the_table;
SELECT * FROM reference_table;
SELECT * FROM citus_local_table;

DELETE FROM the_table WHERE a = 1;
DELETE FROM the_replicated_table WHERE a = 1;
DELETE FROM reference_table WHERE a = 1;
DELETE FROM citus_local_table WHERE a = 1;

SELECT * FROM the_table;
SELECT * FROM reference_table;
SELECT * FROM citus_local_table;

-- drawing from a sequence is not possible
INSERT INTO the_table (a, b) VALUES (1, 2);
INSERT INTO the_replicated_table (a, b) VALUES (1, 2);
INSERT INTO reference_table (a, b) VALUES (1, 2);
INSERT INTO citus_local_table (a, b) VALUES (1, 2);

-- 2PC is not possible
INSERT INTO the_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
INSERT INTO the_replicated_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
INSERT INTO reference_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
INSERT INTO citus_local_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);

-- COPY is not possible because Citus user 2PC
COPY the_table (a, b, z) FROM STDIN WITH CSV;
\.
COPY the_replicated_table (a, b, z) FROM STDIN WITH CSV;
\.
COPY reference_table (a, b, z) FROM STDIN WITH CSV;
\.
COPY citus_local_table (a, b, z) FROM STDIN WITH CSV;
\.

-- all multi-shard modifications require 2PC hence not supported
INSERT INTO the_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
SELECT * FROM the_table ORDER BY a;

-- all modifications to reference tables use 2PC, hence not supported
INSERT INTO reference_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
SELECT * FROM reference_table ORDER BY a;

-- citus local tables are on the coordinator, and coordinator is read-only
INSERT INTO citus_local_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
SELECT * FROM citus_local_table ORDER BY a;

-- modifying CTEs are possible
WITH del AS (DELETE FROM the_table RETURNING *)
SELECT * FROM del ORDER BY a;
WITH del AS (DELETE FROM reference_table RETURNING *)
SELECT * FROM del ORDER BY a;
WITH del AS (DELETE FROM the_replicated_table RETURNING *)
SELECT * FROM del ORDER BY a;
WITH del AS (DELETE FROM citus_local_table RETURNING *)
SELECT * FROM del ORDER BY a;

-- multi-shard COPY is not possible due to 2PC
COPY the_table (a, b, z) FROM STDIN WITH CSV;
\.
COPY reference_table (a, b, z) FROM STDIN WITH CSV;
\.
COPY citus_local_table (a, b, z) FROM STDIN WITH CSV;
\.
SELECT * FROM the_table ORDER BY a;
SELECT * FROM reference_table ORDER BY a;
SELECT * FROM citus_local_table ORDER BY a;
DELETE FROM reference_table;
DELETE FROM citus_local_table;

-- multi-shard modification always uses 2PC, so not supported
DELETE FROM the_table;

-- DDL is not possible
TRUNCATE the_table;
TRUNCATE reference_table;
TRUNCATE citus_local_table;
ALTER TABLE the_table ADD COLUMN c int;
ALTER TABLE reference_table ADD COLUMN c int;
ALTER TABLE citus_local_table ADD COLUMN c int;

-- rollback is possible
BEGIN;
INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);
ROLLBACK;
BEGIN;
INSERT INTO reference_table (a, b, z) VALUES (1, 2, 2);
ROLLBACK;
BEGIN;
INSERT INTO citus_local_table (a, b, z) VALUES (1, 2, 2);
ROLLBACK;

SELECT * FROM the_table ORDER BY a;
SELECT * FROM reference_table ORDER BY a;
SELECT * FROM citus_local_table ORDER BY a;

-- we should still disallow writes to local tables
INSERT INTO local VALUES (1, 1);
INSERT INTO local SELECT i,i FROM generate_series(0,100)i;

-- we shouldn't be able to create local tables
CREATE TEMP TABLE local_copy_of_the_table AS SELECT * FROM the_table;

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"

-- separate follower formations currently cannot do writes
SET citus.writable_standby_coordinator TO on;
INSERT INTO the_table (a, b, z) VALUES (1, 2, 3);
SELECT * FROM the_table ORDER BY a;
INSERT INTO reference_table (a, b, z) VALUES (1, 2, 3);
SELECT * FROM reference_table ORDER BY a;
INSERT INTO citus_local_table (a, b, z) VALUES (1, 2, 3);
SELECT * FROM citus_local_table ORDER BY a;

\c "port=57636 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"

-- when an existing read-replica is forked to become
-- another primary node, we sometimes have to use citus.use_secondary_nodes=always
-- even if the node is not in recovery mode. In those cases, allow LOCK
-- command on local / metadata tables, and also certain UDFs
SHOW citus.use_secondary_nodes;
SELECT pg_is_in_recovery();
SELECT citus_is_coordinator();
BEGIN;
	LOCK TABLE pg_dist_node IN SHARE ROW EXCLUSIVE MODE;
	LOCK TABLE local IN SHARE ROW EXCLUSIVE MODE;
COMMIT;

\c -reuse-previous=off regression - - :master_port
DROP TABLE the_table;
DROP TABLE reference_table;
DROP TABLE citus_local_table;
SELECT master_remove_node('localhost', :master_port);
