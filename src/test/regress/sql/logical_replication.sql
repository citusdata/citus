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

-- Create a publiction and subscription (including replication slot) manually.
-- This allows us to test the cleanup logic at the start of the shard move.
\c - - - :worker_1_port
SET search_path TO logical_replication;
SET citus.enable_ddl_propagation TO off;
CREATE PUBLICATION citus_shard_move_publication_:postgres_oid FOR TABLE dist_6830000;
RESET citus.enable_ddl_propagation;

\c - - - :master_port
SET search_path TO logical_replication;
CREATE TABLE dist_6830000(
    id bigserial PRIMARY KEY
);
\set connection_string '\'user=postgres host=localhost port=' :worker_1_port ' dbname=regression\''
CREATE SUBSCRIPTION citus_shard_move_subscription_:postgres_oid
    CONNECTION :connection_string
    PUBLICATION citus_shard_move_publication_:postgres_oid
    WITH (enabled=false, slot_name=citus_shard_move_slot_:postgres_oid);


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

-- the subscription is still there, as there is no cleanup record for it
-- we have created it manually
SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

\c - - - :worker_1_port
SET search_path TO logical_replication;

-- the publication and repslot are still there, as there are no cleanup records for them
-- we have created them manually
SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

DROP PUBLICATION citus_shard_move_publication_:postgres_oid;
SELECT pg_drop_replication_slot('citus_shard_move_slot_' || :postgres_oid);

\c - - - :worker_2_port
SET search_path TO logical_replication;

SELECT count(*) from pg_subscription;
SELECT count(*) from pg_publication;
SELECT count(*) from pg_replication_slots;
SELECT count(*) from dist;

\c - - - :master_port
SET search_path TO logical_replication;

SET client_min_messages TO WARNING;
ALTER SUBSCRIPTION citus_shard_move_subscription_:postgres_oid DISABLE;
ALTER SUBSCRIPTION citus_shard_move_subscription_:postgres_oid SET (slot_name = NONE);
DROP SUBSCRIPTION citus_shard_move_subscription_:postgres_oid;
DROP SCHEMA logical_replication CASCADE;
SELECT public.wait_for_resource_cleanup();
