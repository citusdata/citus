-- citus--13.1-1--13.2-1

-- bump version to 13.2-1
#include "udfs/worker_last_saved_explain_analyze/13.2-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.citus_rebalance_start(name, boolean, citus.shard_transfer_mode);
#include "udfs/citus_rebalance_start/13.2-1.sql"
#include "udfs/citus_internal_copy_single_shard_placement/13.2-1.sql"
