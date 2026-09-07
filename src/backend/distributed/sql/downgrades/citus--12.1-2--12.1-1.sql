-- citus--12.1-2--12.1-1

-- restore the 3-argument stop_metadata_sync_to_node signature
DROP FUNCTION IF EXISTS pg_catalog.stop_metadata_sync_to_node(text, integer, bool, bool);
#include "../udfs/stop_metadata_sync_to_node/10.2-1.sql"
