CREATE OR REPLACE FUNCTION pg_catalog.extract_node_id_from_global_pid(global_pid bigint)
    RETURNS INTEGER
    LANGUAGE C
AS 'MODULE_PATHNAME', $$extract_node_id_from_global_pid$$;

COMMENT ON FUNCTION pg_catalog.extract_node_id_from_global_pid(global_pid bigint)
    IS 'returns the originator node id for the query with the given global pid';
