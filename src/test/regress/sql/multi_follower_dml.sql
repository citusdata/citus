\c - - :master_host :master_port

CREATE TABLE the_table (a int, b int, z bigserial);
SELECT create_distributed_table('the_table', 'a');

CREATE TABLE local (a int, b int);

\c - - - :follower_master_port

-- inserts normally do not work on a standby coordinator
INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);

-- we can allow DML on a writable standby coordinator
SET citus.writable_standby_coordinator TO on;

INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);
SELECT * FROM the_table;

UPDATE the_table SET z = 3 WHERE a = 1;
SELECT * FROM the_table;

DELETE FROM the_table WHERE a = 1;
SELECT * FROM the_table;

-- drawing from a sequence is not possible
INSERT INTO the_table (a, b) VALUES (1, 2);

-- 2PC is not possible
INSERT INTO the_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);

-- COPY is not possible in 2PC mode
COPY the_table (a, b, z) FROM STDIN WITH CSV;
10,10,10
11,11,11
\.

-- 1PC is possible
SET citus.multi_shard_commit_protocol TO '1pc';
INSERT INTO the_table (a, b, z) VALUES (2, 3, 4), (5, 6, 7);
SELECT * FROM the_table ORDER BY a;

-- modifying CTEs are possible
WITH del AS (DELETE FROM the_table RETURNING *)
SELECT * FROM del ORDER BY a;

-- COPY is possible in 1PC mode
COPY the_table (a, b, z) FROM STDIN WITH CSV;
10,10,10
11,11,11
\.
SELECT * FROM the_table ORDER BY a;
DELETE FROM the_table;

-- DDL is not possible
TRUNCATE the_table;
ALTER TABLE the_table ADD COLUMN c int;

-- rollback is possible
BEGIN;
INSERT INTO the_table (a, b, z) VALUES (1, 2, 2);
ROLLBACK;

SELECT * FROM the_table ORDER BY a;

-- we should still disallow writes to local tables
INSERT INTO local VALUES (1, 1);
INSERT INTO local SELECT a, b FROM the_table;

-- we shouldn't be able to create local tables
CREATE TEMP TABLE local_copy_of_the_table AS SELECT * FROM the_table;

\c "port=9070 dbname=regression options='-c\ citus.use_secondary_nodes=always\ -c\ citus.cluster_name=second-cluster'"

-- separate follower formations currently cannot do writes
SET citus.writable_standby_coordinator TO on;
INSERT INTO the_table (a, b, z) VALUES (1, 2, 3);
SELECT * FROM the_table ORDER BY a;

\c - - :master_host :master_port
DROP TABLE the_table;
