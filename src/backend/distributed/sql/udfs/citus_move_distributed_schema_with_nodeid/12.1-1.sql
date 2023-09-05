CREATE OR REPLACE FUNCTION pg_catalog.citus_move_distributed_schema_with_nodeid(
	schema_id regnamespace,
	target_node_id integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_move_distributed_schema_with_nodeid$$;
COMMENT ON FUNCTION pg_catalog.citus_move_distributed_schema_with_nodeid(
	schema_id regnamespace,
	target_node_id integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a distributed schema to given node';
