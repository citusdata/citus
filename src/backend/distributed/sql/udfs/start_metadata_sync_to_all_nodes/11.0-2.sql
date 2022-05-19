CREATE OR REPLACE FUNCTION pg_catalog.start_metadata_sync_to_all_nodes()
 RETURNS bool
 LANGUAGE C
 STRICT
AS 'MODULE_PATHNAME', $$start_metadata_sync_to_all_nodes$$;
COMMENT ON FUNCTION pg_catalog.start_metadata_sync_to_all_nodes()
 IS 'sync metadata to all active primary nodes';

REVOKE ALL ON FUNCTION pg_catalog.start_metadata_sync_to_all_nodes() FROM PUBLIC;
