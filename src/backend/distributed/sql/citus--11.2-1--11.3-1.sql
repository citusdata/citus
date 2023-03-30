-- citus--11.2-1--11.3-1
#include "udfs/repl_origin_helper/11.3-1.sql"
#include "udfs/worker_adjust_identity_column_seq_ranges/11.3-1.sql"
ALTER TABLE pg_catalog.pg_dist_authinfo REPLICA IDENTITY USING INDEX pg_dist_authinfo_identification_index;
ALTER TABLE pg_catalog.pg_dist_partition REPLICA IDENTITY USING INDEX pg_dist_partition_logical_relid_index;
ALTER TABLE pg_catalog.pg_dist_placement REPLICA IDENTITY USING INDEX pg_dist_placement_placementid_index;
ALTER TABLE pg_catalog.pg_dist_rebalance_strategy REPLICA IDENTITY USING INDEX pg_dist_rebalance_strategy_name_key;
ALTER TABLE pg_catalog.pg_dist_shard REPLICA IDENTITY USING INDEX pg_dist_shard_shardid_index;
ALTER TABLE pg_catalog.pg_dist_transaction REPLICA IDENTITY USING INDEX pg_dist_transaction_unique_constraint;
