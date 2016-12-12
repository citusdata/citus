/* citus--6.1-2--6.1-3.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION stop_metadata_sync_to_node(nodename text, nodeport integer)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$stop_metadata_sync_to_node$$;
COMMENT ON FUNCTION stop_metadata_sync_to_node(nodename text, nodeport integer)                                         
    IS 'stop metadata sync to node';
    
RESET search_path;