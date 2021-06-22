CREATE FUNCTION pg_catalog.stop_metadata_sync_to_node(nodename text, nodeport integer, clear_metadata bool DEFAULT true)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$stop_metadata_sync_to_node$$;
COMMENT ON FUNCTION pg_catalog.stop_metadata_sync_to_node(nodename text, nodeport integer, clear_metadata bool)
    IS 'stop metadata sync to node';
