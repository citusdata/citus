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
AS 'MODULE_PATHNAME', $function$worker_repartition_cleanup$function$;

-- add relations to citus
ALTER EXTENSION citus ADD SCHEMA columnar;
ALTER EXTENSION citus ADD SEQUENCE columnar.storageid_seq;
ALTER EXTENSION citus ADD TABLE columnar.options;
ALTER EXTENSION citus ADD TABLE columnar.stripe;
ALTER EXTENSION citus ADD TABLE columnar.chunk_group;
ALTER EXTENSION citus ADD TABLE columnar.chunk;

ALTER EXTENSION citus ADD FUNCTION columnar.columnar_handler;
ALTER EXTENSION citus ADD ACCESS METHOD columnar;
ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_reset;

ALTER EXTENSION citus ADD FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.columnar_ensure_am_depends_catalog;

DROP FUNCTION pg_catalog.citus_split_shard_by_split_points(
    shard_id bigint,
    split_points text[],
    node_ids integer[],
    shard_transfer_mode citus.shard_transfer_mode);
DROP FUNCTION pg_catalog.worker_split_copy(
    source_shard_id bigint,
    splitCopyInfos pg_catalog.split_copy_info[]);
DROP TYPE pg_catalog.split_copy_info;

DROP FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                     OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                     OUT global_pid int8);
#include "../udfs/get_all_active_transactions/11.0-1.sql"

DROP VIEW pg_catalog.citus_locks;
DROP FUNCTION pg_catalog.citus_locks();
