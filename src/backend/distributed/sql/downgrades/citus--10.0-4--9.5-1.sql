-- citus--10.0-4--9.5-1

-- This migration file aims to fix the issues with upgrades on clusters without public schema.

-- This file is created by the following command, and some more changes in a separate commit
-- cat citus--10.0-3--10.0-2.sql citus--10.0-2--10.0-1.sql citus--10.0-1--9.5-1.sql > citus--10.0-4--9.5-1.sql

-- copy of citus--10.0-4--10.0-3
--
-- 10.0-3--10.0-4 was added later as a patch to fix a bug in our PG upgrade functions
--
-- The upgrade fixes a bug in citus_(prepare|finish)_pg_upgrade. Given the old versions of
-- these functions contain a bug it is better to _not_ restore the old version and keep
-- the patched version of the function.
--
-- This is inline with the downgrade scripts for earlier versions of this patch
--

-- copy of citus--10.0-3--10.0-2
-- this is a downgrade path that will revert the changes made in citus--10.0-2--10.0-3.sql

DROP FUNCTION pg_catalog.citus_update_table_statistics(regclass);

#include "../udfs/citus_update_table_statistics/10.0-1.sql"

CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
RETURNS VOID AS $$
DECLARE
	colocated_tables regclass[];
BEGIN
	SELECT get_colocated_table_array(relation) INTO colocated_tables;

	PERFORM
		master_update_shard_statistics(shardid)
	FROM
		pg_dist_shard
	WHERE
		logicalrelid = ANY (colocated_tables);
END;
$$ LANGUAGE 'plpgsql';
COMMENT ON FUNCTION master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table and its colocated tables';

DROP FUNCTION pg_catalog.citus_get_active_worker_nodes(OUT text, OUT bigint);
-- copy of citus--10.0-2--10.0-1.sql
#include "../../../columnar/sql/downgrades/columnar--10.0-2--10.0-1.sql"

-- copy of citus--10.0-1--9.5-1

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

DROP VIEW IF EXISTS pg_catalog.citus_tables;
DROP VIEW IF EXISTS public.citus_tables;
DROP FUNCTION pg_catalog.alter_distributed_table(regclass, text, int, text, boolean);
DROP FUNCTION pg_catalog.alter_table_set_access_method(regclass, text);
DROP FUNCTION pg_catalog.citus_total_relation_size(regclass,boolean);
DROP FUNCTION pg_catalog.undistribute_table(regclass,boolean);
DROP FUNCTION pg_catalog.citus_add_local_table_to_metadata(regclass,boolean);
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

DROP FUNCTION pg_catalog.fix_pre_citus10_partitioned_table_constraint_names();
DROP FUNCTION pg_catalog.fix_pre_citus10_partitioned_table_constraint_names(regclass);
DROP FUNCTION pg_catalog.worker_fix_pre_citus10_partitioned_table_constraint_names(regclass,bigint,text);
DROP FUNCTION pg_catalog.citus_dist_stat_activity CASCADE;
DROP FUNCTION pg_catalog.citus_worker_stat_activity CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int,
                                                    OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                                    OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                                    OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                                    OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                                    OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$citus_dist_stat_activity$$;

COMMENT ON FUNCTION pg_catalog.citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int,
                                             OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                             OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                             OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                             OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                             OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
IS 'returns distributed transaction activity on distributed tables';

CREATE VIEW citus.citus_dist_stat_activity AS
SELECT * FROM pg_catalog.citus_dist_stat_activity();
ALTER VIEW citus.citus_dist_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_dist_stat_activity TO PUBLIC;

SET search_path = 'pg_catalog';

CREATE VIEW citus.citus_lock_waits AS

WITH
citus_dist_stat_activity AS
(
  SELECT * FROM citus_dist_stat_activity
),
unique_global_wait_edges AS
(
	SELECT DISTINCT ON(waiting_node_id, waiting_transaction_num, blocking_node_id, blocking_transaction_num) * FROM dump_global_wait_edges()
),
citus_dist_stat_activity_with_node_id AS
(
  SELECT
  citus_dist_stat_activity.*, (CASE citus_dist_stat_activity.master_query_host_name WHEN 'coordinator_host' THEN 0 ELSE pg_dist_node.nodeid END) as initiator_node_id
  FROM
  citus_dist_stat_activity LEFT JOIN pg_dist_node
  ON
  citus_dist_stat_activity.master_query_host_name = pg_dist_node.nodename AND
  citus_dist_stat_activity.master_query_host_port = pg_dist_node.nodeport
)
SELECT
 waiting.pid AS waiting_pid,
 blocking.pid AS blocking_pid,
 waiting.query AS blocked_statement,
 blocking.query AS current_statement_in_blocking_process,
 waiting.initiator_node_id AS waiting_node_id,
 blocking.initiator_node_id AS blocking_node_id,
 waiting.master_query_host_name AS waiting_node_name,
 blocking.master_query_host_name AS blocking_node_name,
 waiting.master_query_host_port AS waiting_node_port,
 blocking.master_query_host_port AS blocking_node_port
FROM
 unique_global_wait_edges
JOIN
 citus_dist_stat_activity_with_node_id waiting ON (unique_global_wait_edges.waiting_transaction_num = waiting.transaction_number AND unique_global_wait_edges.waiting_node_id = waiting.initiator_node_id)
JOIN
 citus_dist_stat_activity_with_node_id blocking ON (unique_global_wait_edges.blocking_transaction_num = blocking.transaction_number AND unique_global_wait_edges.blocking_node_id = blocking.initiator_node_id);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

CREATE OR REPLACE FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int,
                                                      OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                                      OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                                      OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                                      OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                                      OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$citus_worker_stat_activity$$;

COMMENT ON FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT master_query_host_name text, OUT master_query_host_port int,
                                               OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                               OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                               OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                               OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                               OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
IS 'returns distributed transaction activity on shards of distributed tables';

CREATE VIEW citus.citus_worker_stat_activity AS
SELECT * FROM pg_catalog.citus_worker_stat_activity();
ALTER VIEW citus.citus_worker_stat_activity SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_worker_stat_activity TO PUBLIC;

RESET search_path;
