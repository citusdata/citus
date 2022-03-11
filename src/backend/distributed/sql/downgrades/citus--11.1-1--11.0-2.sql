CREATE FUNCTION pg_catalog.worker_create_schema(jobid bigint, username text)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_create_schema$function$;

CREATE FUNCTION pg_catalog.worker_cleanup_job_schema_cache()
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_cleanup_job_schema_cache$function$;

CREATE FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[])
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_fetch_foreign_file$function$;

CREATE FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_fetch_partition_file$function$;

CREATE FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_hash_partition_table$function$;

CREATE FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[])
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_merge_files_into_table$function$;

CREATE FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_range_partition_table$function$;

CREATE FUNCTION pg_catalog.worker_repartition_cleanup(bigint)
 RETURNS void
 LANGUAGE c
 STRICT
AS 'MODULE_PATHNAME', $function$worker_repartition_cleanup$function$

ALTER TABLE pg_catalog.pg_dist_local_group DROP COLUMN logical_clock_value;
DROP FUNCTION pg_catalog.get_cluster_clock();
DROP FUNCTION citus_internal.set_transaction_id_clock_value();

DROP FUNCTION IF EXISTS pg_catalog.get_all_active_transactions();
CREATE OR REPLACE FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
								  OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                                  OUT global_pid int8)
RETURNS SETOF RECORD
LANGUAGE C STRICT AS 'MODULE_PATHNAME',
$$get_all_active_transactions$$;

COMMENT ON FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT datname text, OUT process_id int, OUT initiator_node_identifier int4,
							   OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                           OUT global_pid int8)
IS 'returns transaction information for all Citus initiated transactions';

DROP FUNCTION IF EXISTS pg_catalog.get_global_active_transactions();
CREATE OR REPLACE FUNCTION pg_catalog.get_global_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL,
								     OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT global_pid int8)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$get_global_active_transactions$$;
COMMENT ON FUNCTION pg_catalog.get_global_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4, OUT worker_query BOOL,
							      OUT transaction_number int8, OUT transaction_stamp timestamptz, OUT global_pid int8)
     IS 'returns transaction information for all Citus initiated transactions from each node of the cluster';
