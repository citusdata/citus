SET citus.next_shard_id TO 6830000;

CREATE SCHEMA logical_replication;
SET search_path TO logical_replication;

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE dist (
    id bigserial PRIMARY KEY
);

SELECT oid AS postgres_oid FROM pg_roles where rolname = 'postgres' \gset

SELECT create_distributed_table('dist', 'id');
INSERT INTO dist SELECT generate_series(1, 100);

SELECT 1 from citus_add_node('localhost', :master_port, groupId := 0);

\c - - - :worker_1_port
SET search_path TO logical_replication;
CREATE PUBLICATION citus_shard_move_publication_:postgres_oid FOR TABLE dist_6830000;

\c - - - :master_port
SET search_path TO logical_replication;
\set connection_string '\'user=postgres host=localhost port=' :worker_1_port '\''
CREATE SUBSCRIPTION citus_shard_move_subscription_:postgres_oid CONNECTION :connection_string PUBLICATION citus_shard_move_publication_:postgres_oid;

SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) FROM dist;

\c - - - :worker_1_port
SET search_path TO logical_replication;

SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) FROM dist;

\c - - - :master_port
SET search_path TO logical_replication;

select citus_move_shard_placement(6830002, 'localhost', :worker_1_port, 'localhost', :worker_2_port, 'force_logical');

SELECT citus_remove_node('localhost', :master_port);

SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

\c - - - :worker_1_port
SET search_path TO logical_replication;


SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

\c - - - :worker_2_port
SET search_path TO logical_replication;

SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

\c - - - :master_port
SET search_path TO logical_replication;

SET client_min_messages TO WARNING;
DROP SCHEMA logical_replication CASCADE;
