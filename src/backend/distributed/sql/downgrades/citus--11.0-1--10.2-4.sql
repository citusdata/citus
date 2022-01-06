-- citus--11.0-1--10.2-4

DROP FUNCTION pg_catalog.create_distributed_function(regprocedure, text, text, bool);
CREATE FUNCTION pg_catalog.master_apply_delete_command(text)
    RETURNS integer
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_apply_delete_command$$;
COMMENT ON FUNCTION pg_catalog.master_apply_delete_command(text)
    IS 'drop shards matching delete criteria and update metadata';

CREATE FUNCTION pg_catalog.master_get_table_metadata(
                                          relation_name text,
                                          OUT logical_relid oid,
                                          OUT part_storage_type "char",
                                          OUT part_method "char", OUT part_key text,
                                          OUT part_replica_count integer,
                                          OUT part_max_size bigint,
                                          OUT part_placement_policy integer)
    RETURNS record
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$master_get_table_metadata$$;
COMMENT ON FUNCTION master_get_table_metadata(relation_name text)
    IS 'fetch metadata values for the table';
ALTER TABLE pg_catalog.pg_dist_partition DROP COLUMN autoconverted;

CREATE FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    RETURNS real
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_append_table_to_shard$$;
COMMENT ON FUNCTION master_append_table_to_shard(bigint, text, text, integer)
    IS 'append given table to all shard placements and update metadata';

GRANT ALL ON FUNCTION start_metadata_sync_to_node(text, integer) TO PUBLIC;
GRANT ALL ON FUNCTION stop_metadata_sync_to_node(text, integer,bool) TO PUBLIC;

DROP FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer, force bool);
CREATE FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
        RETURNS void
        LANGUAGE C STRICT
        AS 'MODULE_PATHNAME', $$citus_disable_node$$;
COMMENT ON FUNCTION pg_catalog.citus_disable_node(nodename text, nodeport integer)
        IS 'removes node from the cluster temporarily';

DROP FUNCTION pg_catalog.citus_check_connection_to_node (text, integer);
DROP FUNCTION pg_catalog.citus_check_cluster_node_health ();

DROP FUNCTION pg_catalog.citus_internal_add_object_metadata(text, text[], text[], integer, integer, boolean);
DROP FUNCTION pg_catalog.citus_run_local_command(text);
DROP FUNCTION pg_catalog.worker_drop_sequence_dependency(text);

CREATE OR REPLACE VIEW pg_catalog.citus_shards_on_worker AS
	SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r','p','v','m','S','f','')
	      AND n.nspname <> 'pg_catalog'
	      AND n.nspname <> 'information_schema'
	      AND n.nspname !~ '^pg_toast'
          AND pg_catalog.relation_is_a_known_shard(c.oid)
	ORDER BY 1,2;

CREATE OR REPLACE VIEW pg_catalog.citus_shard_indexes_on_worker AS
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner",
 c2.relname as "Table"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_index i ON i.indexrelid = c.oid
     LEFT JOIN pg_catalog.pg_class c2 ON i.indrelid = c2.oid
WHERE c.relkind IN ('i','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
      AND n.nspname !~ '^pg_toast'
      AND pg_catalog.relation_is_a_known_shard(c.oid)
ORDER BY 1,2;

DROP FUNCTION pg_catalog.citus_shards_on_worker();
DROP FUNCTION pg_catalog.citus_shard_indexes_on_worker();
#include "../udfs/create_distributed_function/9.0-1.sql"
ALTER TABLE citus.pg_dist_object DROP COLUMN force_delegation;


SET search_path = 'pg_catalog';


DROP FUNCTION IF EXISTS get_all_active_transactions();


CREATE OR REPLACE FUNCTION get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL,
                                                       OUT transaction_number int8, OUT transaction_stamp timestamptz)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$get_all_active_transactions$$;

COMMENT ON FUNCTION get_all_active_transactions(OUT datid oid, OUT datname text, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL,
                                                OUT transaction_number int8, OUT transaction_stamp timestamptz)
IS 'returns distributed transaction ids of active distributed transactions';

DROP FUNCTION IF EXISTS get_global_active_transactions();

CREATE FUNCTION get_global_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$get_global_active_transactions$$;
 COMMENT ON FUNCTION get_global_active_transactions(OUT database_id oid, OUT process_id int, OUT initiator_node_identifier int4, OUT transaction_number int8, OUT transaction_stamp timestamptz)
     IS 'returns distributed transaction ids of active distributed transactions from each node of the cluster';

RESET search_path;

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

RESET search_path;
