-- citus--9.2-4--9.3-2

-- bump version to 9.3-2

#include "udfs/citus_extradata_container/9.3-2.sql"
#include "udfs/update_distributed_table_colocation/9.3-2.sql"
#include "udfs/replicate_reference_tables/9.3-2.sql"
#include "udfs/citus_remote_connection_stats/9.3-2.sql"
#include "udfs/worker_create_or_alter_role/9.3-2.sql"
#include "udfs/truncate_local_data_after_distributing_table/9.3-2.sql"
-- add citus extension owner as a distributed object, if not already in there
INSERT INTO citus.pg_dist_object SELECT
  (SELECT oid FROM pg_class WHERE relname = 'pg_authid') AS oid,
  (SELECT oid FROM pg_authid WHERE rolname = current_user) as objid,
  0 as objsubid
ON CONFLICT DO NOTHING;
