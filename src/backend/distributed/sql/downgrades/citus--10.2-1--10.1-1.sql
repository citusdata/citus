-- citus--10.2-1--10.1-1

#include "../../../columnar/sql/downgrades/columnar--10.2-1--10.1-1.sql"

DROP FUNCTION pg_catalog.stop_metadata_sync_to_node(text, integer, bool);

CREATE FUNCTION pg_catalog.stop_metadata_sync_to_node(nodename text, nodeport integer)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$stop_metadata_sync_to_node$$;
COMMENT ON FUNCTION pg_catalog.stop_metadata_sync_to_node(nodename text, nodeport integer)
    IS 'stop metadata sync to node';
