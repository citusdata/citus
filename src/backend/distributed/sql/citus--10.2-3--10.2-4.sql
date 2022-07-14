-- citus--10.2-3--10.2-4

-- bump version to 10.2-4

DO $check_columnar$
BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_extension AS e
             INNER JOIN pg_catalog.pg_depend AS d ON (d.refobjid = e.oid)
             INNER JOIN pg_catalog.pg_proc AS p ON (p.oid = d.objid)
             WHERE e.extname='citus_columnar' and p.proname = 'columnar_handler'
  ) THEN
    #include "../../columnar/sql/columnar--10.2-3--10.2-4.sql"
END IF;
END;
$check_columnar$;

#include "udfs/fix_partition_shard_index_names/10.2-4.sql"
#include "udfs/fix_all_partition_shard_index_names/10.2-4.sql"
#include "udfs/worker_fix_partition_shard_index_names/10.2-4.sql"
#include "udfs/citus_finish_pg_upgrade/10.2-4.sql"

