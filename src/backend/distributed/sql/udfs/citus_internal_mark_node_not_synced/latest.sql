CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_mark_node_not_synced(parent_pid int, nodeid int)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_mark_node_not_synced$$;
COMMENT ON FUNCTION citus_internal_mark_node_not_synced(int, int)
    IS 'marks given node not synced by unsetting metadatasynced column at the start of the nontransactional sync.';
