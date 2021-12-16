-- citus--10.1-1--10.0-4

-- This migration file aims to fix the issues with upgrades on clusters without public schema.

-- copy of citus--10.1-1--10.0-3

-- remove databases as distributed objects to prevent unknown object types being managed
-- on older versions.
DELETE FROM citus.pg_dist_object
      WHERE classid = 'pg_catalog.pg_database'::regclass::oid;

#include "../../../columnar/sql/downgrades/columnar--10.1-1--10.0-3.sql"

DROP FUNCTION pg_catalog.create_distributed_table(regclass, text, citus.distribution_type, text, int);
CREATE FUNCTION create_distributed_table(table_name regclass,
										 distribution_column text,
										 distribution_type citus.distribution_type DEFAULT 'hash',
										 colocate_with text DEFAULT 'default')
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$create_distributed_table$$;
COMMENT ON FUNCTION create_distributed_table(table_name regclass,
											 distribution_column text,
											 distribution_type citus.distribution_type,
											 colocate_with text)
    IS 'creates a distributed table';

DROP FUNCTION pg_catalog.worker_partitioned_relation_total_size(regclass);
DROP FUNCTION pg_catalog.worker_partitioned_relation_size(regclass);
DROP FUNCTION pg_catalog.worker_partitioned_table_size(regclass);
DROP FUNCTION pg_catalog.citus_local_disk_space_stats();

#include "../udfs/citus_prepare_pg_upgrade/9.5-1.sql"
#include "../udfs/citus_finish_pg_upgrade/10.0-1.sql"
#include "../udfs/get_rebalance_table_shards_plan/9.2-1.sql"

-- the migration for citus_add_rebalance_strategy from 9.2-1 was the first one,
-- so it doesn't have a DROP. This is why we DROP manually here.
DROP FUNCTION pg_catalog.citus_add_rebalance_strategy;
#include "../udfs/citus_add_rebalance_strategy/9.2-1.sql"

ALTER TABLE pg_catalog.pg_dist_rebalance_strategy DROP COLUMN improvement_threshold;

-- the migration for get_rebalance_progress from 9.0-1 was the first one,
-- so it doesn't have a DROP. This is why we DROP manually here.
DROP FUNCTION pg_catalog.get_rebalance_progress;
#include "../udfs/get_rebalance_progress/9.0-1.sql"

CREATE OR REPLACE VIEW pg_catalog.citus_shards AS
WITH shard_sizes AS (SELECT * FROM pg_catalog.citus_shard_sizes())
SELECT
     pg_dist_shard.logicalrelid AS table_name,
     pg_dist_shard.shardid,
     shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) as shard_name,
     CASE WHEN partkey IS NOT NULL THEN 'distributed' WHEN repmodel = 't' THEN 'reference' ELSE 'local' END AS citus_table_type,
     colocationid AS colocation_id,
     pg_dist_node.nodename,
     pg_dist_node.nodeport,
     (SELECT size FROM shard_sizes WHERE
       shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) = table_name
       OR
       'public.' || shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) = table_name
      LIMIT 1) as shard_size
FROM
   pg_dist_shard
JOIN
   pg_dist_placement
ON
   pg_dist_shard.shardid = pg_dist_placement.shardid
JOIN
   pg_dist_node
ON
   pg_dist_placement.groupid = pg_dist_node.groupid
JOIN
   pg_dist_partition
ON
   pg_dist_partition.logicalrelid = pg_dist_shard.logicalrelid
ORDER BY
   pg_dist_shard.logicalrelid::text, shardid
;

#include "../udfs/citus_finish_pg_upgrade/10.0-1.sql"
CREATE FUNCTION citus_internal.pg_dist_rebalance_strategy_enterprise_check()
  RETURNS TRIGGER
  LANGUAGE C
  AS 'MODULE_PATHNAME';
CREATE TRIGGER pg_dist_rebalance_strategy_enterprise_check_trigger
  BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON pg_dist_rebalance_strategy
  FOR EACH STATEMENT EXECUTE FUNCTION citus_internal.pg_dist_rebalance_strategy_enterprise_check();

DROP PROCEDURE pg_catalog.citus_cleanup_orphaned_shards();
