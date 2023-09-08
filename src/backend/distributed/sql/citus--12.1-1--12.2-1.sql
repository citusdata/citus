-- citus--12.1-1--12.2-1

-- bump version to 12.2-1

#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

ALTER TABLE pg_dist_placement ADD COLUMN needsisolatednode boolean NOT NULL DEFAULT false;

-- Drop the legacy one that survived from 10.2-1, not the one created in 11.2-1.
--
-- And as we did when upgrading from 10.2-1 to 11.2-1, citus_internal_add_placement_metadata/12.2-1.sql
-- preserves the one created in 11.2-1 as the "new legacy" one.
DROP FUNCTION pg_catalog.citus_internal_add_placement_metadata(
							shard_id bigint, shard_state integer,
							shard_length bigint, group_id integer,
							placement_id bigint);

#include "udfs/citus_internal_add_placement_metadata/12.2-1.sql"

#include "udfs/citus_internal_shard_group_set_needsisolatednode/12.2-1.sql"

#include "udfs/citus_shard_set_isolated/12.2-1.sql"
#include "udfs/citus_shard_unset_isolated/12.2-1.sql"
