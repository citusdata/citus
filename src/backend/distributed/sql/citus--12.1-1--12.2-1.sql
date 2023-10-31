-- citus--12.1-1--12.2-1

-- bump version to 12.2-1

#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

ALTER TABLE pg_dist_shard ADD COLUMN needsseparatenode boolean NOT NULL DEFAULT false;

DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text);
#include "udfs/citus_internal_add_shard_metadata/12.2-1.sql"

#include "udfs/citus_internal_shard_property_set/12.2-1.sql"
#include "udfs/citus_shard_property_set/12.2-1.sql"

DROP VIEW citus_shards;
#include "udfs/citus_shards/12.2-1.sql"
