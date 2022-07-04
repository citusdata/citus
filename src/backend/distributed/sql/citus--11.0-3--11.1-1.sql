#include "udfs/citus_locks/11.1-1.sql"

DROP FUNCTION pg_catalog.worker_create_schema(bigint,text);
DROP FUNCTION pg_catalog.worker_cleanup_job_schema_cache();
DROP FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);
DROP FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer);
DROP FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[]);
DROP FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_repartition_cleanup(bigint);

DO $check_columnar$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
    #include "../../columnar/sql/columnar--11.0-3--11.1-1.sql"
END IF;
END;
$check_columnar$;

-- If upgrading citus, the columnar objects are already being a part of the
-- citus extension, and must be detached so that they can be attached
-- to the citus_columnar extension.
DO $check_citus$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus' and p.proname = 'columnar_handler'
  ) THEN
    ALTER EXTENSION citus DROP SCHEMA columnar;
    ALTER EXTENSION citus DROP SCHEMA columnar_internal;
    ALTER EXTENSION citus DROP SEQUENCE columnar_internal.storageid_seq;

    -- columnar tables
    ALTER EXTENSION citus DROP TABLE columnar_internal.options;
    ALTER EXTENSION citus DROP TABLE columnar_internal.stripe;
    ALTER EXTENSION citus DROP TABLE columnar_internal.chunk_group;
    ALTER EXTENSION citus DROP TABLE columnar_internal.chunk;

    ALTER EXTENSION citus DROP FUNCTION columnar_internal.columnar_handler;
    ALTER EXTENSION citus DROP ACCESS METHOD columnar;
    ALTER EXTENSION citus DROP FUNCTION pg_catalog.alter_columnar_table_set;
    ALTER EXTENSION citus DROP FUNCTION pg_catalog.alter_columnar_table_reset;
    ALTER EXTENSION citus DROP FUNCTION columnar.get_storage_id;

    -- columnar view
    ALTER EXTENSION citus DROP VIEW columnar.storage;
    ALTER EXTENSION citus DROP VIEW columnar.options;
    ALTER EXTENSION citus DROP VIEW columnar.stripe;
    ALTER EXTENSION citus DROP VIEW columnar.chunk_group;
    ALTER EXTENSION citus DROP VIEW columnar.chunk;

    -- functions under citus_internal for columnar
    ALTER EXTENSION citus DROP FUNCTION citus_internal.upgrade_columnar_storage;
    ALTER EXTENSION citus DROP FUNCTION citus_internal.downgrade_columnar_storage;
    ALTER EXTENSION citus DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog;

  END IF;
END $check_citus$;
#include "udfs/citus_finish_pg_upgrade/11.1-1.sql"

DROP FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                     OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                     OUT global_pid int8);
#include "udfs/get_all_active_transactions/11.1-1.sql"
#include "udfs/citus_split_shard_by_split_points/11.1-1.sql"
#include "udfs/worker_split_copy/11.1-1.sql"
