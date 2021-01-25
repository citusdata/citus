-- citus--10.0-1--9.5-1

-- In Citus 10.0, we added another internal udf (notify_constraint_dropped)
-- to be called by citus_drop_trigger. Since this script is executed when
-- downgrading Citus, we don't have notify_constraint_dropped in citus.so.
-- For this reason, we first need to downgrade citus_drop_trigger so it doesn't
-- call notify_constraint_dropped.
-- To downgrade citus_drop_trigger, we first need to have the old version of
-- citus_drop_all_shards as we renamed it in Citus 10.0.
ALTER FUNCTION pg_catalog.citus_drop_all_shards(regclass, text, text)
RENAME TO master_drop_all_shards;
#include "../udfs/citus_drop_trigger/9.5-1.sql"

-- Now we can safely drop notify_constraint_dropped as we downgraded citus_drop_trigger.
DROP FUNCTION pg_catalog.notify_constraint_dropped();

#include "../udfs/citus_finish_pg_upgrade/9.5-1.sql"

#include "../../../columnar/sql/downgrades/columnar--10.0-1--9.5-1.sql"

DROP VIEW public.citus_tables;
DROP FUNCTION pg_catalog.alter_distributed_table(regclass, text, int, text, boolean);
DROP FUNCTION pg_catalog.alter_table_set_access_method(regclass, text);
DROP FUNCTION pg_catalog.citus_total_relation_size(regclass,boolean);
DROP FUNCTION pg_catalog.undistribute_table(regclass,boolean);
DROP FUNCTION pg_catalog.create_citus_local_table(regclass,boolean);
DROP FUNCTION pg_catalog.citus_add_node(text, integer, integer, noderole, name);
DROP FUNCTION pg_catalog.citus_activate_node(text, integer);
DROP FUNCTION pg_catalog.citus_add_inactive_node(text, integer, integer, noderole, name);
DROP FUNCTION pg_catalog.citus_add_secondary_node(text, integer, text, integer, name);
DROP FUNCTION pg_catalog.citus_disable_node(text, integer);
DROP FUNCTION pg_catalog.citus_drain_node(text, integer, citus.shard_transfer_mode, name);
DROP FUNCTION pg_catalog.citus_remove_node(text, integer);
DROP FUNCTION pg_catalog.citus_set_node_property(text, integer, text, boolean);
DROP FUNCTION pg_catalog.citus_unmark_object_distributed(oid, oid, int);
DROP FUNCTION pg_catalog.citus_update_node(int, text, int, bool, int);
DROP FUNCTION pg_catalog.citus_update_shard_statistics(bigint);
DROP FUNCTION pg_catalog.citus_update_table_statistics(regclass);
DROP FUNCTION pg_catalog.citus_copy_shard_placement(bigint, text, integer, text, integer, bool, citus.shard_transfer_mode);
DROP FUNCTION pg_catalog.citus_move_shard_placement(bigint, text, integer, text, integer, citus.shard_transfer_mode);

ALTER FUNCTION pg_catalog.citus_conninfo_cache_invalidate()
RENAME TO master_conninfo_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_local_group_cache_invalidate()
RENAME TO master_dist_local_group_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_node_cache_invalidate()
RENAME TO master_dist_node_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_object_cache_invalidate()
RENAME TO master_dist_object_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_partition_cache_invalidate()
RENAME TO master_dist_partition_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_placement_cache_invalidate()
RENAME TO master_dist_placement_cache_invalidate;
ALTER FUNCTION pg_catalog.citus_dist_shard_cache_invalidate()
RENAME TO master_dist_shard_cache_invalidate;

#include "../udfs/citus_conninfo_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_local_group_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_node_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_object_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_partition_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_placement_cache_invalidate/9.5-1.sql"
#include "../udfs/citus_dist_shard_cache_invalidate/9.5-1.sql"

DROP VIEW pg_catalog.time_partitions;
DROP FUNCTION pg_catalog.time_partition_range(regclass);
DROP PROCEDURE pg_catalog.alter_old_partitions_set_access_method(regclass,timestamptz,name);

DROP FUNCTION pg_catalog.citus_set_coordinator_host(text,int,noderole,name);
DROP FUNCTION pg_catalog.worker_change_sequence_dependency(regclass, regclass, regclass);

CREATE FUNCTION pg_catalog.mark_tables_colocated(source_table_name regclass, target_table_names regclass[])
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$mark_tables_colocated$$;
COMMENT ON FUNCTION pg_catalog.mark_tables_colocated(source_table_name regclass, target_table_names regclass[])
	IS 'mark target distributed tables as colocated with the source table';

CREATE FUNCTION pg_catalog.master_modify_multiple_shards(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_modify_multiple_shards$$;
COMMENT ON FUNCTION master_modify_multiple_shards(text)
    IS 'push delete and update queries to shards';

CREATE FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                           distribution_column text,
                                                           distribution_method citus.distribution_type)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_create_distributed_table$$;
COMMENT ON FUNCTION pg_catalog.master_create_distributed_table(table_name regclass,
                                                               distribution_column text,
                                                               distribution_method citus.distribution_type)
    IS 'define the table distribution functions';

CREATE FUNCTION pg_catalog.master_create_worker_shards(table_name text, shard_count integer,
                                                       replication_factor integer DEFAULT 2)
    RETURNS void
    AS 'MODULE_PATHNAME'
    LANGUAGE C STRICT;

DROP FUNCTION pg_catalog.remove_local_tables_from_metadata();

#include "../udfs/citus_total_relation_size/7.0-1.sql"
#include "../udfs/upgrade_to_reference_table/8.0-1.sql"
#include "../udfs/undistribute_table/9.5-1.sql"
#include "../udfs/create_citus_local_table/9.5-1.sql"
DROP VIEW pg_catalog.citus_shards CASCADE;
DROP FUNCTION pg_catalog.citus_shard_sizes(OUT table_name text, OUT size bigint);
