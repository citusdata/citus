-- citus--10.2-3--10.2-4

-- bump version to 10.2-4

#include "../../columnar/sql/columnar--10.2-3--10.2-4.sql"

#include "udfs/fix_partition_shard_index_names/10.2-4.sql"
#include "udfs/fix_all_partition_shard_index_names/10.2-4.sql"
#include "udfs/worker_fix_partition_shard_index_names/10.2-4.sql"
#include "udfs/citus_finish_pg_upgrade/10.2-4.sql"
