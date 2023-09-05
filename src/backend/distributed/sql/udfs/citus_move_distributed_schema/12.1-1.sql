-- citus_move_distributed_schema, using target node name and node port
CREATE OR REPLACE FUNCTION pg_catalog.citus_move_distributed_schema(
	schema_id regnamespace,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_move_distributed_schema$$;
COMMENT ON FUNCTION pg_catalog.citus_move_distributed_schema(
	schema_id regnamespace,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a distributed schema to given node';

-- citus_move_distributed_schema, using target node id
CREATE OR REPLACE FUNCTION pg_catalog.citus_move_distributed_schema(
	schema_id regnamespace,
	target_node_id integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_move_distributed_schema_with_nodeid$$;
COMMENT ON FUNCTION pg_catalog.citus_move_distributed_schema(
	schema_id regnamespace,
	target_node_id integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a distributed schema to given node';
