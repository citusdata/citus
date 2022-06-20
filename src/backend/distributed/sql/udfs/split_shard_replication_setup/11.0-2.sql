DROP TYPE IF EXISTS citus.split_shard_info;

DROP FUNCTION IF EXISTS pg_catalog.worker_split_shard_replication_setup;

CREATE TYPE citus.split_shard_info AS (
    source_shard_id bigint,
    child_shard_id bigint,
    shard_min_value integer,
    shard_max_value integer,
    node_id integer);


CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_replication_setup(
    splitShardInfo citus.split_shard_info[])
RETURNS bigint
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_replication_setup(splitShardInfo citus.split_shard_info[])
    IS 'Replication setup for splitting a shard'
