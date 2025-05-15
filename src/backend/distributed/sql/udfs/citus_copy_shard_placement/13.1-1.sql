CREATE OR REPLACE FUNCTION pg_catalog.citus_copy_one_shard_placement(
	shard_id bigint,
	source_node_id integer,
	target_node_id integer,
	flags integer,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_copy_one_shard_placement$$;
COMMENT ON FUNCTION pg_catalog.citus_copy_one_shard_placement(
	shard_id bigint,
	source_node_id integer,
	target_node_id integer,
	flags integer,
	transfer_mode citus.shard_transfer_mode)
IS 'copy a single shard from the source node to the destination node';
