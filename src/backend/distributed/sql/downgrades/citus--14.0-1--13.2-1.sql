-- citus--14.0-1--13.2-1
-- downgrade version to 13.2-1

#include "../udfs/citus_prepare_pg_upgrade/13.0-1.sql"
#include "../udfs/citus_finish_pg_upgrade/13.2-1.sql"

DROP AGGREGATE IF EXISTS pg_catalog.worker_binary_partial_agg(oid, anyelement);
DROP AGGREGATE IF EXISTS pg_catalog.coord_binary_combine_agg(oid, bytea, anyelement);
DROP FUNCTION IF EXISTS pg_catalog.worker_binary_partial_agg_ffunc(internal);
DROP FUNCTION IF EXISTS pg_catalog.coord_binary_combine_agg_sfunc(internal, oid, bytea, anyelement);
DROP FUNCTION IF EXISTS pg_catalog.coord_binary_combine_agg_ffunc(internal, oid, bytea, anyelement);
