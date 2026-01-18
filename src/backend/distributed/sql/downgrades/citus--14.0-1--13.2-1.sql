-- citus--14.0-1--13.2-1
-- downgrade version to 13.2-1

#include "../udfs/citus_prepare_pg_upgrade/13.0-1.sql"
#include "../udfs/citus_finish_pg_upgrade/13.2-1.sql"

DROP AGGREGATE IF EXISTS pg_catalog.worker_binary_partial_agg(oid, anyelement);
DROP AGGREGATE IF EXISTS pg_catalog.coord_binary_combine_agg(oid, bytea, anyelement);
DROP FUNCTION IF EXISTS pg_catalog.worker_binary_partial_agg_ffunc(internal);
DROP FUNCTION IF EXISTS pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement);
DROP FUNCTION IF EXISTS pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement);

#include "../udfs/citus_finish_citus_upgrade/11.0-2.sql"
DROP FUNCTION IF EXISTS pg_catalog.fix_pre_citus14_colocation_group_collation_mismatches();
