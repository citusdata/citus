DROP FUNCTION pg_catalog.worker_create_schema(bigint,text);
DROP FUNCTION pg_catalog.worker_cleanup_job_schema_cache();
DROP FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);
DROP FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer);
DROP FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[]);
DROP FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_repartition_cleanup(bigint);

ALTER TABLE pg_catalog.pg_dist_local_group ADD COLUMN logical_clock_value BIGINT NOT NULL DEFAULT 0;
UPDATE pg_catalog.pg_dist_local_group SET logical_clock_value = extract(epoch from now()) * 1000;

#include "udfs/get_cluster_clock/11.1-1.sql"
#include "udfs/set_transaction_id_clock_value/11.1-1.sql"
#include "udfs/get_all_active_transactions/11.1-1.sql"
#include "udfs/get_global_active_transactions/11.1-1.sql"
