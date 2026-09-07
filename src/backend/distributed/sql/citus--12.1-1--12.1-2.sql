-- citus--12.1-1--12.1-2

-- bump version to 12.1-2

-- stop_metadata_sync_to_node gains an optional drop_orphaned_shell_tables argument;
-- drop the old 3-argument signature first so the defaulted call sites stay unambiguous.
DROP FUNCTION IF EXISTS pg_catalog.stop_metadata_sync_to_node(text, integer, bool);
#include "udfs/stop_metadata_sync_to_node/12.1-2.sql"
