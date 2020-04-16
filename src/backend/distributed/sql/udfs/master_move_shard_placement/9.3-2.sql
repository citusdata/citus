CREATE OR REPLACE FUNCTION pg_catalog.master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$master_move_shard_placement$$;

COMMENT ON FUNCTION pg_catalog.master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a shard from a the source node to the destination node';
