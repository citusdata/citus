-- citus--10.2-3--11.0-1

-- bump version to 11.0-1

#include "udfs/fix_partition_shard_index_names/11.0-1.sql"
#include "udfs/fix_all_partition_shard_index_names/11.0-1.sql"
#include "udfs/worker_fix_partition_shard_index_names/11.0-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.master_apply_delete_command(text);
