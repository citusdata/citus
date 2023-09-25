-- citus--12.1-1--12.2-1

-- bump version to 12.2-1

#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

ALTER TABLE pg_dist_shard ADD COLUMN needsisolatednode boolean NOT NULL DEFAULT false;

#include "udfs/citus_internal_add_shard_metadata/12.2-1.sql"

#include "udfs/citus_internal_shard_group_set_needsisolatednode/12.2-1.sql"

#include "udfs/citus_shard_set_isolated/12.2-1.sql"
#include "udfs/citus_shard_unset_isolated/12.2-1.sql"
