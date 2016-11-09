/* citus--6.0-18--6.1-1.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION start_metadata_sync_to_node(nodename text, nodeport integer)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$start_metadata_sync_to_node$$;
COMMENT ON FUNCTION start_metadata_sync_to_node(nodename text, nodeport integer)                                         
    IS 'sync metadata to node';
    
RESET search_path;
