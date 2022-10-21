#include "udfs/citus_locks/11.1-1.sql"
#include "udfs/citus_tables/11.1-1.sql"
#include "udfs/citus_shards/11.1-1.sql"
#include "udfs/create_distributed_table_concurrently/11.1-1.sql"
#include "udfs/citus_internal_delete_partition_metadata/11.1-1.sql"
#include "udfs/citus_copy_shard_placement/11.1-1.sql"

-- We should not introduce breaking sql changes to upgrade files after they are released.
-- We did that for worker_fetch_foreign_file in v9.0.0 and worker_repartition_cleanup in v9.2.0.
-- When we try to drop those udfs in that file, they were missing for some clients unexpectedly
-- due to buggy changes in old upgrade scripts. For that case, the fix is to change DROP statements
-- with DROP IF EXISTS for those 2 udfs in 11.0-4--11.1-1.
-- Fixes an upgrade problem for worker_fetch_foreign_file when upgrade starts from 8.3 up to 11.1
-- Fixes an upgrade problem for worker_repartition_cleanup when upgrade starts from 9.1 up to 11.1
-- Refer the related PR https://github.com/citusdata/citus/pull/6441 for more information
DROP FUNCTION IF EXISTS pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);
DROP FUNCTION IF EXISTS pg_catalog.worker_repartition_cleanup(bigint);

DROP FUNCTION pg_catalog.worker_create_schema(bigint,text);
DROP FUNCTION pg_catalog.worker_cleanup_job_schema_cache();
DROP FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer);
DROP FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[]);
DROP FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray);

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
#include "udfs/citus_prepare_pg_upgrade/11.1-1.sql"
#include "udfs/citus_finish_pg_upgrade/11.1-1.sql"

DROP FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                     OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                     OUT global_pid int8);
#include "udfs/get_all_active_transactions/11.1-1.sql"
#include "udfs/citus_split_shard_by_split_points/11.1-1.sql"
#include "udfs/worker_split_copy/11.1-1.sql"
#include "udfs/worker_copy_table_to_node/11.1-1.sql"
#include "udfs/worker_split_shard_replication_setup/11.1-1.sql"
#include "udfs/citus_isolation_test_session_is_blocked/11.1-1.sql"
#include "udfs/replicate_reference_tables/11.1-1.sql"
#include "udfs/worker_split_shard_release_dsm/11.1-1.sql"

DROP FUNCTION pg_catalog.isolate_tenant_to_new_shard(table_name regclass, tenant_id "any", cascade_option text);
#include "udfs/isolate_tenant_to_new_shard/11.1-1.sql"

-- Table of records to:
-- 1) Cleanup leftover resources after a failure
-- 2) Deferred drop of old shard placements after a split.
#include "udfs/citus_cleanup_orphaned_resources/11.1-1.sql"

CREATE TABLE citus.pg_dist_cleanup (
    record_id bigint primary key,
    operation_id bigint not null,
    object_type int not null,
    object_name text not null,
    node_group_id int not null,
    policy_type int not null
);
ALTER TABLE citus.pg_dist_cleanup SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_cleanup TO public;

-- Sequence used to generate operation Ids and record Ids in pg_dist_cleanup_record.
CREATE SEQUENCE citus.pg_dist_operationid_seq;
ALTER SEQUENCE citus.pg_dist_operationid_seq SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_operationid_seq TO public;

CREATE SEQUENCE citus.pg_dist_cleanup_recordid_seq;
ALTER SEQUENCE citus.pg_dist_cleanup_recordid_seq SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_cleanup_recordid_seq TO public;

-- We recreate these two UDF from 11.0-1 on purpose, because we changed their
-- old definition. By recreating it here upgrades also pick up the new changes.
#include "udfs/pg_cancel_backend/11.0-1.sql"
#include "udfs/pg_terminate_backend/11.0-1.sql"

CREATE TYPE citus.citus_job_status AS ENUM ('scheduled', 'running', 'finished', 'cancelling', 'cancelled', 'failing', 'failed');
ALTER TYPE citus.citus_job_status SET SCHEMA pg_catalog;

CREATE TABLE citus.pg_dist_background_job (
    job_id bigserial NOT NULL,
    state pg_catalog.citus_job_status DEFAULT 'scheduled' NOT NULL,
    job_type name NOT NULL,
    description text NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,

    CONSTRAINT pg_dist_background_job_pkey PRIMARY KEY (job_id)
);
ALTER TABLE citus.pg_dist_background_job SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_background_job TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_dist_background_job_job_id_seq TO PUBLIC;

CREATE TYPE citus.citus_task_status AS ENUM ('blocked', 'runnable', 'running', 'done', 'cancelling', 'error', 'unscheduled', 'cancelled');
ALTER TYPE citus.citus_task_status SET SCHEMA pg_catalog;

CREATE TABLE citus.pg_dist_background_task(
    job_id bigint NOT NULL REFERENCES pg_catalog.pg_dist_background_job(job_id),
    task_id bigserial NOT NULL,
    owner regrole NOT NULL DEFAULT CURRENT_USER::regrole,
    pid integer,
    status pg_catalog.citus_task_status default 'runnable' NOT NULL,
    command text NOT NULL,
    retry_count integer,
    not_before timestamptz, -- can be null to indicate no delay for start of the task, will be set on failure to delay retries
    message text NOT NULL DEFAULT '',

    CONSTRAINT pg_dist_background_task_pkey PRIMARY KEY (task_id),
    CONSTRAINT pg_dist_background_task_job_id_task_id UNIQUE (job_id, task_id) -- required for FK's to enforce tasks only reference other tasks within the same job
);
ALTER TABLE citus.pg_dist_background_task SET SCHEMA pg_catalog;
CREATE INDEX pg_dist_background_task_status_task_id_index ON pg_catalog.pg_dist_background_task USING btree(status, task_id);
GRANT SELECT ON pg_catalog.pg_dist_background_task TO PUBLIC;
GRANT SELECT ON pg_catalog.pg_dist_background_task_task_id_seq TO PUBLIC;

CREATE TABLE citus.pg_dist_background_task_depend(
   job_id bigint NOT NULL REFERENCES pg_catalog.pg_dist_background_job(job_id) ON DELETE CASCADE,
   task_id bigint NOT NULL,
   depends_on bigint NOT NULL,

   PRIMARY KEY (job_id, task_id, depends_on),
   FOREIGN KEY (job_id, task_id) REFERENCES pg_catalog.pg_dist_background_task (job_id, task_id) ON DELETE CASCADE,
   FOREIGN KEY (job_id, depends_on) REFERENCES pg_catalog.pg_dist_background_task (job_id, task_id) ON DELETE CASCADE
);

ALTER TABLE citus.pg_dist_background_task_depend SET SCHEMA pg_catalog;
CREATE INDEX pg_dist_background_task_depend_task_id ON pg_catalog.pg_dist_background_task_depend USING btree(job_id, task_id);
CREATE INDEX pg_dist_background_task_depend_depends_on ON pg_catalog.pg_dist_background_task_depend USING btree(job_id, depends_on);
GRANT SELECT ON pg_catalog.pg_dist_background_task_depend TO PUBLIC;

#include "udfs/citus_job_wait/11.1-1.sql"
#include "udfs/citus_job_cancel/11.1-1.sql"
#include "udfs/citus_rebalance_start/11.1-1.sql"
#include "udfs/citus_rebalance_stop/11.1-1.sql"
#include "udfs/citus_rebalance_wait/11.1-1.sql"
#include "udfs/get_rebalance_progress/11.1-1.sql"
