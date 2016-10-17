/* citus--6.0-6--6.0-7.sql */

CREATE FUNCTION pg_catalog.get_colocated_table_array(regclass)
    RETURNS regclass[]
    AS 'citus'
    LANGUAGE C STRICT;

CREATE OR REPLACE FUNCTION pg_catalog.master_move_shard_placement(shard_id bigint,
													   source_node_name text,
													   source_node_port integer,
													   target_node_name text,
													   target_node_port integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_move_shard_placement$$;
COMMENT ON FUNCTION pg_catalog.master_move_shard_placement(shard_id bigint,
													   source_node_name text,
													   source_node_port integer,
													   target_node_name text,
													   target_node_port integer)
    IS 'move shard from remote node';
