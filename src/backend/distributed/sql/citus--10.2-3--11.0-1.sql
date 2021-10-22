-- citus--10.2-3--11.0-1

-- bump version to 11.0-1

#include "udfs/fix_partition_shard_index_names/11.0-1.sql"
#include "udfs/fix_all_partition_shard_index_names/11.0-1.sql"
#include "udfs/worker_fix_partition_shard_index_names/11.0-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.master_apply_delete_command(text);
DROP FUNCTION pg_catalog.master_get_table_metadata(text);
ALTER TABLE pg_catalog.pg_dist_partition ADD COLUMN autoconverted boolean DEFAULT false;

REVOKE ALL ON FUNCTION start_metadata_sync_to_node(text, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION stop_metadata_sync_to_node(text, integer,bool) FROM PUBLIC;
