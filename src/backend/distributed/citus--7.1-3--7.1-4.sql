/* citus--7.1-3--7.1-4 */

CREATE TYPE citus.shard_transfer_mode AS ENUM (
   'auto',
   'force_logical',
   'block_writes'
);

SET search_path = 'pg_catalog';

DROP FUNCTION master_move_shard_placement(bigint, text, integer, text, integer);
DROP FUNCTION master_copy_shard_placement(bigint, text, integer, text, integer, bool);

CREATE OR REPLACE FUNCTION master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$master_move_shard_placement$$;

COMMENT ON FUNCTION master_move_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'move a shard from a the source node to the destination node';

CREATE FUNCTION master_copy_shard_placement(
	shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	do_repair bool DEFAULT true,
	transfer_mode citus.shard_transfer_mode default 'auto')
RETURNS void
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$master_copy_shard_placement$$;

COMMENT ON FUNCTION master_copy_shard_placement(shard_id bigint,
	source_node_name text,
	source_node_port integer,
	target_node_name text,
	target_node_port integer,
	do_repair bool,
	shard_transfer_mode citus.shard_transfer_mode)
IS 'copy a shard from the source node to the destination node';

RESET search_path;
