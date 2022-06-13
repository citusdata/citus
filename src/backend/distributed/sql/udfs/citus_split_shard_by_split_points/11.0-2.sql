CREATE OR REPLACE FUNCTION pg_catalog.citus_split_shard_by_split_points(
    shard_id bigint,
    split_points integer[],
    -- A 'nodeId' is a uint32 in CITUS [1, 4294967296] but postgres does not have unsigned type support.
    -- Use integer (consistent with other previously defined UDFs that take nodeId as integer) as for all practical purposes it is big enough.
    node_ids integer[],
    -- Three modes to be implemented: blocking, non_blocking and auto.
    -- Currently, the default / only supported mode is blocking.
    split_mode citus.split_mode default 'blocking')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_split_shard_by_split_points$$;
COMMENT ON FUNCTION pg_catalog.citus_split_shard_by_split_points(shard_id bigint, split_points integer[], nodeIds integer[], citus.split_mode)
    IS 'split a shard using split mode.';
