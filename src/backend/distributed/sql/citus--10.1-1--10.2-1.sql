-- citus--10.1-1--10.2-1

-- bump version to 10.2-1

DROP FUNCTION IF EXISTS pg_catalog.stop_metadata_sync_to_node(text, integer);
GRANT ALL ON FUNCTION pg_catalog.worker_record_sequence_dependency(regclass,regclass,name) TO PUBLIC;

-- the same shard cannot have placements on different nodes
ALTER TABLE pg_catalog.pg_dist_placement ADD CONSTRAINT placement_shardid_groupid_unique_index UNIQUE (shardid, groupid);

#include "udfs/stop_metadata_sync_to_node/10.2-1.sql"
--#include "../../columnar/sql/columnar--10.1-1--10.2-1.sql"
DO $check_columnar$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
      #include "../../columnar/sql/columnar--10.1-1--10.2-1.sql"
  END IF;
END;
$check_columnar$;

#include "udfs/citus_internal_add_partition_metadata/10.2-1.sql";
#include "udfs/citus_internal_add_shard_metadata/10.2-1.sql";
#include "udfs/citus_internal_add_placement_metadata/10.2-1.sql";
#include "udfs/citus_internal_update_placement_metadata/10.2-1.sql";
#include "udfs/citus_internal_delete_shard_metadata/10.2-1.sql";
#include "udfs/citus_internal_update_relation_colocation/10.2-1.sql";
#include "udfs/create_time_partitions/10.2-1.sql"
#include "udfs/drop_old_time_partitions/10.2-1.sql"
#include "udfs/get_missing_time_partition_ranges/10.2-1.sql"
#include "udfs/worker_nextval/10.2-1.sql"


DROP FUNCTION pg_catalog.citus_drop_all_shards(regclass, text, text);
CREATE FUNCTION pg_catalog.citus_drop_all_shards(logicalrelid regclass,
                                                 schema_name text,
                                                 table_name text,
                                                 drop_shards_metadata_only boolean default false)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_drop_all_shards$$;
COMMENT ON FUNCTION pg_catalog.citus_drop_all_shards(regclass, text, text, boolean)
    IS 'drop all shards in a relation and update metadata';
#include "udfs/citus_drop_trigger/10.2-1.sql";
#include "udfs/citus_prepare_pg_upgrade/10.2-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.2-1.sql"

