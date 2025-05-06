CREATE OR REPLACE FUNCTION citus_internal.citus_internal_copy_single_shard_placement(
	shard_id bigint,
	source_node_id integer,
	target_node_id integer,
	flags integer,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_internal_copy_single_shard_placement$$;
