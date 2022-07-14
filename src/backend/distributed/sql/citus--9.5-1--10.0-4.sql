-- citus--9.5-1--10.0-4

-- This migration file aims to fix the issues with upgrades on clusters without public schema.

-- This file is created by the following command, and some more changes in a separate commit
-- cat citus--9.5-1--10.0-1.sql citus--10.0-1--10.0-2.sql citus--10.0-2--10.0-3.sql > citus--9.5-1--10.0-4.sql

-- copy of citus--9.5-1--10.0-1
DROP FUNCTION pg_catalog.upgrade_to_reference_table(regclass);
DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);

#include "udfs/citus_total_relation_size/10.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"
#include "udfs/alter_distributed_table/10.0-1.sql"
#include "udfs/alter_table_set_access_method/10.0-1.sql"
#include "udfs/undistribute_table/10.0-1.sql"
#include "udfs/create_citus_local_table/10.0-1.sql"
#include "udfs/citus_set_coordinator_host/10.0-1.sql"
#include "udfs/citus_add_node/10.0-1.sql"
#include "udfs/citus_activate_node/10.0-1.sql"
#include "udfs/citus_add_inactive_node/10.0-1.sql"
#include "udfs/citus_add_secondary_node/10.0-1.sql"
#include "udfs/citus_disable_node/10.0-1.sql"
#include "udfs/citus_drain_node/10.0-1.sql"
#include "udfs/citus_remove_node/10.0-1.sql"
#include "udfs/citus_set_node_property/10.0-1.sql"
#include "udfs/citus_unmark_object_distributed/10.0-1.sql"
#include "udfs/citus_update_node/10.0-1.sql"
#include "udfs/citus_update_shard_statistics/10.0-1.sql"
#include "udfs/citus_update_table_statistics/10.0-1.sql"
#include "udfs/citus_copy_shard_placement/10.0-1.sql"
#include "udfs/citus_move_shard_placement/10.0-1.sql"
#include "udfs/citus_drop_trigger/10.0-1.sql"
#include "udfs/worker_change_sequence_dependency/10.0-1.sql"
#include "udfs/remove_local_tables_from_metadata/10.0-1.sql"

--#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"
DO $check_columnar$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"
END IF;
END;
$check_columnar$;

#include "udfs/time_partition_range/10.0-1.sql"
#include "udfs/time_partitions/10.0-1.sql"
#include "udfs/alter_old_partitions_set_access_method/10.0-1.sql"

ALTER FUNCTION pg_catalog.master_conninfo_cache_invalidate()
RENAME TO citus_conninfo_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_local_group_cache_invalidate()
RENAME TO citus_dist_local_group_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_node_cache_invalidate()
RENAME TO citus_dist_node_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_object_cache_invalidate()
RENAME TO citus_dist_object_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_partition_cache_invalidate()
RENAME TO citus_dist_partition_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_placement_cache_invalidate()
RENAME TO citus_dist_placement_cache_invalidate;
ALTER FUNCTION pg_catalog.master_dist_shard_cache_invalidate()
RENAME TO citus_dist_shard_cache_invalidate;

#include "udfs/citus_conninfo_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_local_group_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_node_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_object_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_partition_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_placement_cache_invalidate/10.0-1.sql"
#include "udfs/citus_dist_shard_cache_invalidate/10.0-1.sql"

ALTER FUNCTION pg_catalog.master_drop_all_shards(regclass, text, text)
RENAME TO citus_drop_all_shards;

DROP FUNCTION pg_catalog.master_modify_multiple_shards(text);
DROP FUNCTION pg_catalog.master_create_distributed_table(regclass, text, citus.distribution_type);
DROP FUNCTION pg_catalog.master_create_worker_shards(text, integer, integer);
DROP FUNCTION pg_catalog.mark_tables_colocated(regclass, regclass[]);
#include "udfs/citus_shard_sizes/10.0-1.sql"
#include "udfs/citus_shards/10.0-1.sql"

#include "udfs/fix_pre_citus10_partitioned_table_constraint_names/10.0-1.sql"
#include "udfs/worker_fix_pre_citus10_partitioned_table_constraint_names/10.0-1.sql"
DROP FUNCTION pg_catalog.citus_dist_stat_activity CASCADE;

CREATE OR REPLACE FUNCTION pg_catalog.citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
                                                    OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                                    OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                                    OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                                    OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                                    OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$citus_dist_stat_activity$$;

COMMENT ON FUNCTION pg_catalog.citus_dist_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
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
  citus_dist_stat_activity.*, (CASE citus_dist_stat_activity.distributed_query_host_name WHEN 'coordinator_host' THEN 0 ELSE pg_dist_node.nodeid END) as initiator_node_id
  FROM
  citus_dist_stat_activity LEFT JOIN pg_dist_node
  ON
  citus_dist_stat_activity.distributed_query_host_name = pg_dist_node.nodename AND
  citus_dist_stat_activity.distributed_query_host_port = pg_dist_node.nodeport
)
SELECT
 waiting.pid AS waiting_pid,
 blocking.pid AS blocking_pid,
 waiting.query AS blocked_statement,
 blocking.query AS current_statement_in_blocking_process,
 waiting.initiator_node_id AS waiting_node_id,
 blocking.initiator_node_id AS blocking_node_id,
 waiting.distributed_query_host_name AS waiting_node_name,
 blocking.distributed_query_host_name AS blocking_node_name,
 waiting.distributed_query_host_port AS waiting_node_port,
 blocking.distributed_query_host_port AS blocking_node_port
FROM
 unique_global_wait_edges
JOIN
 citus_dist_stat_activity_with_node_id waiting ON (unique_global_wait_edges.waiting_transaction_num = waiting.transaction_number AND unique_global_wait_edges.waiting_node_id = waiting.initiator_node_id)
JOIN
 citus_dist_stat_activity_with_node_id blocking ON (unique_global_wait_edges.blocking_transaction_num = blocking.transaction_number AND unique_global_wait_edges.blocking_node_id = blocking.initiator_node_id);

ALTER VIEW citus.citus_lock_waits SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_lock_waits TO PUBLIC;

DROP FUNCTION citus_worker_stat_activity CASCADE;

CREATE OR REPLACE FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
                                                      OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT datid oid, OUT datname name,
                                                      OUT pid int, OUT usesysid oid, OUT usename name, OUT application_name text, OUT client_addr INET,
                                                      OUT client_hostname TEXT, OUT client_port int, OUT backend_start timestamptz, OUT xact_start timestamptz,
                                                      OUT query_start timestamptz, OUT state_change timestamptz, OUT wait_event_type text, OUT wait_event text,
                                                      OUT state text, OUT backend_xid xid, OUT backend_xmin xid, OUT query text, OUT backend_type text)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$citus_worker_stat_activity$$;

COMMENT ON FUNCTION citus_worker_stat_activity(OUT query_hostname text, OUT query_hostport int, OUT distributed_query_host_name text, OUT distributed_query_host_port int,
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

-- copy of citus--10.0-1--10.0-2

--#include "../../columnar/sql/columnar--10.0-1--10.0-2.sql"
DO $check_columnar$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
#include "../../columnar/sql/columnar--10.0-1--10.0-2.sql"
END IF;
END;
$check_columnar$;


-- copy of citus--10.0-2--10.0-3

#include "udfs/citus_update_table_statistics/10.0-3.sql"

CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_update_table_statistics$$;
COMMENT ON FUNCTION pg_catalog.master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table';

CREATE OR REPLACE FUNCTION pg_catalog.citus_get_active_worker_nodes(OUT node_name text, OUT node_port bigint)
    RETURNS SETOF record
    LANGUAGE C STRICT ROWS 100
    AS 'MODULE_PATHNAME', $$citus_get_active_worker_nodes$$;
COMMENT ON FUNCTION pg_catalog.citus_get_active_worker_nodes()
    IS 'fetch set of active worker nodes';

-- copy of citus--10.0-3--10.0-4

-- This migration file aims to fix 2 issues with upgrades on clusters

-- 1. a bug in public schema dependency for citus_tables view.
--
-- Users who do not have public schema in their clusters were unable to upgrade
-- to Citus 10.x due to the citus_tables view that used to be created in public
-- schema

#include "udfs/citus_tables/10.0-4.sql"

-- 2. a bug in our PG upgrade functions
--
-- Users who took the 9.5-2--10.0-1 upgrade path already have the fix, but users
-- who took the 9.5-1--10.0-1 upgrade path do not. Hence, we repeat the CREATE OR
-- REPLACE from the 9.5-2 definition for citus_prepare_pg_upgrade.

#include "udfs/citus_prepare_pg_upgrade/9.5-2.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-4.sql"


RESET search_path;

