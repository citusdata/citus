DROP FUNCTION pg_catalog.citus_copy_shard_placement;
CREATE FUNCTION pg_catalog.citus_copy_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_copy_shard_placement$$;

COMMENT ON FUNCTION pg_catalog.citus_copy_shard_placement(shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'copy a shard from the source node to the destination node';
