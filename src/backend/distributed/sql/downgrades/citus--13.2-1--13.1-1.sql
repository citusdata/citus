-- citus--13.2-1--13.1-1
-- downgrade version to 13.1-1
DROP FUNCTION IF EXISTS citus_internal.citus_internal_copy_single_shard_placement(bigint, integer, integer, integer, citus.shard_transfer_mode);
#include "udfs/citus_rebalance_start/11.1-1.sql"

DROP FUNCTION IF EXISTS pg_catalog.worker_last_saved_explain_analyze();
#include "../udfs/worker_last_saved_explain_analyze/9.4-1.sql"
