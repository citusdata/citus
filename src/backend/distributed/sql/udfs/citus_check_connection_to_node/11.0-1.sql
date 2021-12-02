CREATE FUNCTION pg_catalog.citus_check_connection_to_node (
    nodename text,
    nodeport integer DEFAULT 5432)
    RETURNS bool
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME', $$citus_check_connection_to_node$$;

COMMENT ON FUNCTION pg_catalog.citus_check_connection_to_node (
    nodename text, nodeport integer)
    IS 'checks connection to another node';
