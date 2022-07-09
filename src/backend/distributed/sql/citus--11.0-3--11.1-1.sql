DROP FUNCTION pg_catalog.worker_create_schema(bigint,text);
DROP FUNCTION pg_catalog.worker_cleanup_job_schema_cache();
DROP FUNCTION pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);
DROP FUNCTION pg_catalog.worker_fetch_partition_file(bigint, integer, integer, integer, text, integer);
DROP FUNCTION pg_catalog.worker_hash_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_merge_files_into_table(bigint, integer, text[], text[]);
DROP FUNCTION pg_catalog.worker_range_partition_table(bigint, integer, text, text, oid, anyarray);
DROP FUNCTION pg_catalog.worker_repartition_cleanup(bigint);

#include "../../columnar/sql/columnar--11.0-3--11.1-1.sql"

DROP FUNCTION pg_catalog.get_all_active_transactions(OUT datid oid, OUT process_id int, OUT initiator_node_identifier int4,
                                                     OUT worker_query BOOL, OUT transaction_number int8, OUT transaction_stamp timestamptz,
                                                     OUT global_pid int8);
#include "udfs/get_all_active_transactions/11.1-1.sql"
#include "udfs/citus_split_shard_by_split_points/11.1-1.sql"
#include "udfs/worker_split_copy/11.1-1.sql"
