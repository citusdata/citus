-- We want to create the type in pg_catalog but doing that leads to an error
-- "ERROR:  permission denied to create "pg_catalog.split_copy_info"
-- "DETAIL:  System catalog modifications are currently disallowed. ""
-- As a workaround, we create the type in the citus schema and then later modify it to pg_catalog.
DROP TYPE IF EXISTS citus.split_copy_info;
CREATE TYPE citus.split_copy_info AS (
    destination_shard_id bigint,
    destination_shard_min_value text,
    destination_shard_max_value text,
    -- A 'nodeId' is a uint32 in CITUS [1, 4294967296] but postgres does not have unsigned type support.
    -- Use integer (consistent with other previously defined UDFs that take nodeId as integer) as for all practical purposes it is big enough.
    destination_shard_node_id integer);
ALTER TYPE citus.split_copy_info SET SCHEMA pg_catalog;

CREATE OR REPLACE FUNCTION pg_catalog.worker_split_copy(
    source_shard_id bigint,
	distribution_column text,
    splitCopyInfos pg_catalog.split_copy_info[])
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$worker_split_copy$$;
COMMENT ON FUNCTION pg_catalog.worker_split_copy(source_shard_id bigint, distribution_column text, splitCopyInfos pg_catalog.split_copy_info[])
    IS 'Perform split copy for shard';
