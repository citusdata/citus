-- citus_move_shard_placement, but with nodeid
CREATE FUNCTION pg_catalog.citus_move_shard_placement(
	shard_id bigint,
	source_node_id integer,
	target_node_id integer,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_move_shard_placement_with_nodeid$$;

COMMENT ON FUNCTION pg_catalog.citus_move_shard_placement(
	shard_id bigint,
	source_node_id integer,
	target_node_id integer,
	transfer_mode citus.shard_transfer_mode)
IS 'move a shard from the source node to the destination node';

CREATE OR REPLACE FUNCTION pg_catalog.citus_move_shard_placement(
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
