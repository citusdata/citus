CREATE TYPE citus.split_shard_info AS (
    source_shard_id bigint,
    child_shard_id bigint,
    shard_min_value text,
    shard_max_value text,
    node_id integer);

CREATE TYPE citus.replication_slot_info AS(node_id integer, slot_owner text, slot_name text);

CREATE OR REPLACE FUNCTION pg_catalog.worker_split_shard_replication_setup(
    splitShardInfo citus.split_shard_info[])
RETURNS setof citus.replication_slot_info
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_shard_replication_setup$$;
COMMENT ON FUNCTION pg_catalog.worker_split_shard_replication_setup(splitShardInfo citus.split_shard_info[])
    IS 'Replication setup for splitting a shard';
    IS 'Replication setup for splitting a shard';

REVOKE ALL ON FUNCTION pg_catalog.worker_split_shard_replication_setup(citus.split_shard_info[]) FROM PUBLIC;
