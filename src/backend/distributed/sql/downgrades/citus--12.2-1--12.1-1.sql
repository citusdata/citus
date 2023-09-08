-- citus--12.2-1--12.1-1

#include "../udfs/citus_add_rebalance_strategy/10.1-1.sql"

ALTER TABLE pg_dist_placement DROP COLUMN needsisolatednode;

DROP FUNCTION pg_catalog.citus_internal_add_placement_metadata(
							shard_id bigint,
							shard_length bigint, group_id integer,
							placement_id bigint,
                            needs_isolated_node boolean);

#include "../udfs/citus_internal_add_placement_metadata/11.2-1.sql"

DROP FUNCTION pg_catalog.citus_shard_set_isolated(shard_id bigint);
DROP FUNCTION pg_catalog.citus_shard_unset_isolated(shard_id bigint);

DROP FUNCTION pg_catalog.citus_internal_shard_group_set_needsisolatednode(
                            shard_id bigint,
                            enabled boolean);
