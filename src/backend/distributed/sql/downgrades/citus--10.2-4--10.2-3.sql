-- citus--10.2-4--10.2-3

DROP FUNCTION pg_catalog.fix_all_partition_shard_index_names();
DROP FUNCTION pg_catalog.fix_partition_shard_index_names(regclass);
DROP FUNCTION pg_catalog.worker_fix_partition_shard_index_names(regclass, text, text);

#include "../udfs/citus_finish_pg_upgrade/10.2-1.sql"

-- This needs to be done after downgrading citus_finish_pg_upgrade. This is
-- because citus_finish_pg_upgrade/10.2-4 depends on columnar_ensure_am_depends_catalog,
-- which is dropped by columnar--10.2-4--10.2-3.sql
#include "../../../columnar/sql/downgrades/columnar--10.2-4--10.2-3.sql"
