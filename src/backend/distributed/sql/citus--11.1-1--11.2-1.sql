-- citus--11.1-1--11.2-1

DROP FUNCTION pg_catalog.worker_append_table_to_shard(text, text, text, integer);

#include "udfs/get_rebalance_progress/11.2-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.2-1.sql"
#include "datatypes/citus_cluster_clock/11.2-1.sql"
#include "udfs/citus_get_node_clock/11.2-1.sql"
#include "udfs/citus_get_transaction_clock/11.2-1.sql"
#include "udfs/citus_is_clock_after/11.2-1.sql"
#include "udfs/citus_internal_adjust_local_clock_to_remote/11.2-1.sql"
#include "udfs/worker_split_shard_replication_setup/11.2-1.sql"
