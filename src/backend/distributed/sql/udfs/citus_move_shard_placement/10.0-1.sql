CREATE FUNCTION pg_catalog.citus_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_move_shard_placement$$;

COMMENT ON FUNCTION pg_catalog.citus_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a shard from a the source node to the destination node';
