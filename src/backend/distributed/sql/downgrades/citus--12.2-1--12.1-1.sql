-- citus--12.2-1--12.1-1

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

DROP VIEW pg_catalog.citus_shards;
#include "../udfs/citus_shards/12.0-1.sql"

DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, boolean);
#include "../udfs/citus_internal_add_shard_metadata/10.2-1.sql"

DROP FUNCTION pg_catalog.citus_shard_property_set(shard_id bigint, anti_affinity boolean);

DROP FUNCTION pg_catalog.citus_internal_shard_property_set(
                            shard_id bigint,
                            needs_separate_node boolean);

ALTER TABLE pg_dist_shard DROP COLUMN needsseparatenode;
