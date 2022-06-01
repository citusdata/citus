CREATE SCHEMA resync_metadata_with_sequences;
SET search_path TO resync_metadata_with_sequences;

CREATE TABLE test_serial(a bigserial PRIMARY KEY);
SELECT create_distributed_table('test_serial', 'a');

CREATE SEQUENCE myseq;
CREATE TABLE test_sequence(a bigint DEFAULT nextval('myseq'));
SELECT create_distributed_table('test_sequence', 'a');

CREATE SEQUENCE myseq_ref;
CREATE TABLE test_serial_ref(a bigserial PRIMARY KEY, b bigint DEFAULT nextval('myseq_ref'));
SELECT create_reference_table('test_serial_ref');

INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;
INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;

SET client_min_messages TO ERROR;
SELECT 1 FROM citus_add_node('localhost', :master_port, groupid:=0);

CREATE SEQUENCE myseq_locl_to_dist;
CREATE TABLE test_local_to_dist(a bigserial PRIMARY KEY, b bigint DEFAULT nextval('myseq_locl_to_dist'));
SELECT citus_add_local_table_to_metadata('test_local_to_dist');
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

SET citus.shard_replication_factor TO 1;
CREATE SEQUENCE other_id_seq;
CREATE TABLE sensors(
measureid bigserial,
other_id bigint DEFAULT nextval('other_id_seq'),
eventdatetime date) PARTITION BY RANGE(eventdatetime);

CREATE TABLE sensors_old PARTITION OF sensors FOR VALUES FROM ('2000-01-01') TO ('2020-01-01');
CREATE TABLE sensors_2020_01_01 PARTITION OF sensors FOR VALUES FROM ('2020-01-01') TO ('2020-02-01');
CREATE TABLE sensors_news PARTITION OF sensors FOR VALUES FROM ('2020-05-01') TO ('2025-01-01');
SELECT create_distributed_table('sensors', 'measureid', colocate_with:='none');

\c - - - :worker_1_port
SET search_path tO resync_metadata_with_sequences;

INSERT INTO test_serial VALUES(DEFAULT) RETURNING *;
INSERT INTO test_serial VALUES(DEFAULT) RETURNING *;

INSERT into test_sequence VALUES(DEFAULT) RETURNING *;
INSERT into test_sequence VALUES(DEFAULT) RETURNING *;

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;
INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;

INSERT INTO sensors VALUES (DEFAULT, DEFAULT, '2010-01-01') RETURNING *;
INSERT INTO sensors_news VALUES (DEFAULT, DEFAULT, '2021-01-01') RETURNING *;

\c - - - :master_port
SET client_min_messages TO ERROR;
SELECT 1 FROM citus_activate_node('localhost', :worker_1_port);


\c - - - :worker_1_port
SET search_path tO resync_metadata_with_sequences;

-- can continue inserting with the existing sequence/serial
INSERT INTO test_serial VALUES(DEFAULT) RETURNING *;
INSERT INTO test_serial VALUES(DEFAULT) RETURNING *;

INSERT into test_sequence VALUES(DEFAULT) RETURNING *;
INSERT into test_sequence VALUES(DEFAULT) RETURNING *;

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;
INSERT INTO test_serial_ref VALUES(DEFAULT) RETURNING *;

INSERT INTO sensors VALUES (DEFAULT, DEFAULT, '2010-01-01') RETURNING *;
INSERT INTO sensors_news VALUES (DEFAULT, DEFAULT, '2021-01-01') RETURNING *;

\c - - - :master_port
SET search_path tO resync_metadata_with_sequences;
SELECT create_distributed_table('test_local_to_dist', 'a', colocate_with:='none');

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

\c - - - :worker_1_port
SET search_path tO resync_metadata_with_sequences;

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

\c - - - :master_port
SET search_path tO resync_metadata_with_sequences;

SELECT alter_distributed_table('test_local_to_dist', shard_count:=6);

SET citus.shard_replication_factor TO 1;
SELECT alter_distributed_table('sensors', shard_count:=5);

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

INSERT INTO sensors VALUES (DEFAULT, DEFAULT, '2010-01-01') RETURNING *;
INSERT INTO sensors_news VALUES (DEFAULT, DEFAULT, '2021-01-01') RETURNING *;

\c - - - :worker_1_port
SET search_path tO resync_metadata_with_sequences;

INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;
INSERT INTO test_local_to_dist VALUES(DEFAULT) RETURNING *;

INSERT INTO sensors VALUES (DEFAULT, DEFAULT, '2010-01-01') RETURNING *;
INSERT INTO sensors_news VALUES (DEFAULT, DEFAULT, '2021-01-01') RETURNING *;

\c - - - :master_port
SET search_path tO resync_metadata_with_sequences;

DROP TABLE test_serial, test_sequence;

\c - - - :worker_1_port
SET search_path tO resync_metadata_with_sequences;

-- show that we only have the sequences left after
-- dropping the tables (e.g., bigserial is dropped)
select count(*) from pg_sequences where schemaname ilike '%resync_metadata_with_sequences%';

\c - - - :master_port
SET client_min_messages TO ERROR;
SELECT 1 FROM citus_remove_node('localhost', :master_port);

DROP SCHEMA resync_metadata_with_sequences CASCADE;
