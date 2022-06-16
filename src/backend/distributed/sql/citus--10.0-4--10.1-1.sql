-- citus--10.0-4--10.1-1

-- add the current database to the distributed objects if not already in there.
-- this is to reliably propagate some of the alter database commands that might be
-- supported.

INSERT INTO citus.pg_dist_object SELECT
  'pg_catalog.pg_database'::regclass::oid AS oid,
  (SELECT oid FROM pg_database WHERE datname = current_database()) as objid,
  0 as objsubid
ON CONFLICT DO NOTHING;

--#include "../../columnar/sql/columnar--10.0-3--10.1-1.sql"
DO $check_columnar$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
      #include "../../columnar/sql/columnar--10.0-3--10.1-1.sql"
  END IF;
END;
$check_columnar$;
#include "udfs/create_distributed_table/10.1-1.sql";
#include "udfs/worker_partitioned_relation_total_size/10.1-1.sql"
#include "udfs/worker_partitioned_relation_size/10.1-1.sql"
#include "udfs/worker_partitioned_table_size/10.1-1.sql"
#include "udfs/citus_prepare_pg_upgrade/10.1-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.1-1.sql"
#include "udfs/citus_local_disk_space_stats/10.1-1.sql"
#include "udfs/get_rebalance_table_shards_plan/10.1-1.sql"
#include "udfs/citus_add_rebalance_strategy/10.1-1.sql"

ALTER TABLE pg_catalog.pg_dist_rebalance_strategy ADD COLUMN improvement_threshold float4 NOT NULL default 0;
UPDATE pg_catalog.pg_dist_rebalance_strategy SET improvement_threshold = 0.5 WHERE name = 'by_disk_size';

#include "udfs/get_rebalance_progress/10.1-1.sql"

-- use streaming replication when replication factor = 1
WITH replicated_shards AS (
    SELECT shardid
    FROM pg_dist_placement
    WHERE shardstate = 1 OR shardstate = 3
    GROUP BY shardid
    HAVING count(*) <> 1 ),
replicated_relations AS (
    SELECT DISTINCT logicalrelid
    FROM pg_dist_shard
    JOIN replicated_shards
    USING (shardid)
)
UPDATE pg_dist_partition
SET repmodel = 's'
WHERE repmodel = 'c'
    AND partmethod = 'h'
    AND logicalrelid NOT IN (SELECT * FROM replicated_relations);
#include "udfs/citus_shards/10.1-1.sql"

DROP TRIGGER pg_dist_rebalance_strategy_enterprise_check_trigger ON pg_catalog.pg_dist_rebalance_strategy;
DROP FUNCTION citus_internal.pg_dist_rebalance_strategy_enterprise_check();

#include "udfs/citus_cleanup_orphaned_shards/10.1-1.sql"
