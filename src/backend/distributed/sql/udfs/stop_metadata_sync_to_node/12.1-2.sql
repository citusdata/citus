CREATE OR REPLACE FUNCTION pg_catalog.stop_metadata_sync_to_node(
    nodename text,
    nodeport integer,
    clear_metadata bool DEFAULT true,
    drop_orphaned_shell_tables bool DEFAULT false)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$stop_metadata_sync_to_node$$;
COMMENT ON FUNCTION pg_catalog.stop_metadata_sync_to_node(nodename text, nodeport integer, clear_metadata bool, drop_orphaned_shell_tables bool)
    IS 'stop metadata sync to node';
