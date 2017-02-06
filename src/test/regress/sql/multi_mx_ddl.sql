-- Tests related to distributed DDL commands on mx cluster

ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1600000;

SELECT * FROM mx_ddl_table ORDER BY key;

-- CREATE INDEX
CREATE INDEX ddl_test_index ON mx_ddl_table(value);

CREATE INDEX CONCURRENTLY ddl_test_concurrent_index ON mx_ddl_table(value);

-- ADD COLUMN
ALTER TABLE mx_ddl_table ADD COLUMN version INTEGER;

-- SET DEFAULT
ALTER TABLE mx_ddl_table ALTER COLUMN version SET DEFAULT 1; 

SELECT master_modify_multiple_shards('UPDATE mx_ddl_table SET version=0.1 WHERE version IS NULL');

-- SET NOT NULL
ALTER TABLE mx_ddl_table ALTER COLUMN version SET NOT NULL;


-- See that the changes are applied on coordinator, worker tables and shards
\d mx_ddl_table

\c - - - :worker_1_port

\d mx_ddl_table

\d mx_ddl_table_1220088

\c - - - :worker_2_port

\d mx_ddl_table

\d mx_ddl_table_1220089

INSERT INTO mx_ddl_table VALUES (37, 78, 2);
INSERT INTO mx_ddl_table VALUES (38, 78);

-- Switch to the coordinator
\c - - - :master_port


-- SET DATA TYPE
ALTER TABLE mx_ddl_table ALTER COLUMN version SET DATA TYPE double precision;

INSERT INTO mx_ddl_table VALUES (78, 83, 2.1);

\c - - - :worker_1_port
SELECT * FROM mx_ddl_table ORDER BY key;

-- Switch to the coordinator
\c - - - :master_port

-- DROP INDEX
DROP INDEX ddl_test_index;

DROP INDEX CONCURRENTLY ddl_test_concurrent_index;

-- DROP DEFAULT
ALTER TABLE mx_ddl_table ALTER COLUMN version DROP DEFAULT;

-- DROP NOT NULL
ALTER TABLE mx_ddl_table ALTER COLUMN version DROP NOT NULL;

-- DROP COLUMN
ALTER TABLE mx_ddl_table DROP COLUMN version;


-- See that the changes are applied on coordinator, worker tables and shards
\d mx_ddl_table

\c - - - :worker_1_port

\d mx_ddl_table

\d mx_ddl_table_1220088

\c - - - :worker_2_port

\d mx_ddl_table

\d mx_ddl_table_1220089

-- Show that DDL commands are done within a two-phase commit transaction
\c - - - :master_port

SET client_min_messages TO debug2;

CREATE INDEX ddl_test_index ON mx_ddl_table(value);

RESET client_min_messages;

DROP INDEX ddl_test_index;

-- show that sequences owned by mx tables result in unique values
SET citus.shard_replication_factor TO 1;
SET citus.shard_count TO 4;
SET citus.replication_model TO streaming;
CREATE TABLE mx_sequence(key INT, value BIGSERIAL);
SELECT create_distributed_table('mx_sequence', 'key');

\c - - - :worker_1_port

SELECT groupid FROM pg_dist_local_group;
SELECT * FROM mx_sequence_value_seq;

\c - - - :worker_2_port

SELECT groupid FROM pg_dist_local_group;
SELECT * FROM mx_sequence_value_seq;

\c - - - :master_port

-- the type of sequences can't be changed
ALTER TABLE mx_sequence ALTER value TYPE BIGINT;
ALTER TABLE mx_sequence ALTER value TYPE INT;

