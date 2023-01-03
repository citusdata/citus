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
    distribution_column text,
    splitCopyInfos pg_catalog.split_copy_info[]);
DROP TYPE pg_catalog.split_copy_info;

DROP FUNCTION pg_catalog.worker_copy_table_to_node(
    source_table regclass,
    target_node_id integer);

DROP FUNCTION pg_catalog.worker_split_shard_replication_setup(
    splitShardInfo pg_catalog.split_shard_info[]);
DROP TYPE pg_catalog.split_shard_info;
DROP TYPE pg_catalog.replication_slot_info;

DROP FUNCTION pg_catalog.worker_split_shard_release_dsm();

DROP FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                     OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                     OUT global_pid int8);
#include "../udfs/get_all_active_transactions/11.0-1.sql"

DROP VIEW pg_catalog.citus_locks;
DROP FUNCTION pg_catalog.citus_locks();

#include "../udfs/citus_tables/10.0-4.sql"
#include "../udfs/citus_shards/10.1-1.sql"

DROP FUNCTION pg_catalog.replicate_reference_tables(citus.shard_transfer_mode);
#include "../udfs/replicate_reference_tables/9.3-2.sql"

DROP FUNCTION pg_catalog.isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text, shard_transfer_mode citus.shard_transfer_mode);
#include "../udfs/isolate_tenant_to_new_shard/8.0-1.sql"
DROP FUNCTION pg_catalog.create_distributed_table_concurrently;
DROP FUNCTION pg_catalog.citus_internal_delete_partition_metadata(regclass);

-- Check if user has any cleanup records.
-- If not, DROP pg_dist_cleanup and continue safely.
-- Otherwise, raise an exception to stop the downgrade process.
DO $$
DECLARE
    cleanup_record_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO cleanup_record_count FROM pg_dist_cleanup;

    IF cleanup_record_count = 0 THEN
        -- no cleanup records exist, can safely downgrade
        DROP TABLE pg_catalog.pg_dist_cleanup;
    ELSE
        RAISE EXCEPTION 'pg_dist_cleanup is introduced in Citus 11.1'
        USING HINT = 'To downgrade Citus to an older version, you should '
                     'first cleanup all the orphaned resources and make sure '
                     'pg_dist_cleanup is empty, by executing '
                     'CALL citus_cleanup_orphaned_resources();';
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP SEQUENCE pg_catalog.pg_dist_operationid_seq;
DROP SEQUENCE pg_catalog.pg_dist_cleanup_recordid_seq;
DROP PROCEDURE pg_catalog.citus_cleanup_orphaned_resources();

DROP FUNCTION pg_catalog.citus_rebalance_start(name, bool, citus.shard_transfer_mode);
DROP FUNCTION pg_catalog.citus_rebalance_stop();
DROP FUNCTION pg_catalog.citus_rebalance_wait();
DROP FUNCTION pg_catalog.citus_job_cancel(bigint);
DROP FUNCTION pg_catalog.citus_job_wait(bigint, pg_catalog.citus_job_status);
DROP TABLE pg_catalog.pg_dist_background_task_depend;
DROP TABLE pg_catalog.pg_dist_background_task;
DROP TYPE pg_catalog.citus_task_status;
DROP TABLE pg_catalog.pg_dist_background_job;
DROP TYPE pg_catalog.citus_job_status;
DROP FUNCTION pg_catalog.citus_copy_shard_placement;
#include "../udfs/citus_copy_shard_placement/10.0-1.sql"
#include "../udfs/get_rebalance_progress/10.1-1.sql"
