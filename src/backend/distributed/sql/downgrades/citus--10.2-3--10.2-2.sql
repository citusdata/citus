-- citus--10.2-3--10.2-2

#include "../../../columnar/sql/downgrades/columnar--10.2-3--10.2-2.sql"
DROP FUNCTION pg_catalog.fix_all_partition_shard_index_names();
DROP FUNCTION pg_catalog.fix_partition_shard_index_names(regclass);
DROP FUNCTION pg_catalog.worker_fix_partition_shard_index_names(regclass, text, text);
