-- citus--10.1-1--10.2-1

-- bump version to 10.2-1

DROP FUNCTION IF EXISTS pg_catalog.stop_metadata_sync_to_node(text, integer);

#include "udfs/stop_metadata_sync_to_node/10.2-1.sql"
#include "../../columnar/sql/columnar--10.1-1--10.2-1.sql"
