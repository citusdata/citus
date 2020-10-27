-- citus--9.4-1--9.5-1

-- bump version to 9.5-1
#include "udfs/undistribute_table/9.5-1.sql"
#include "udfs/create_citus_local_table/9.5-1.sql"
#include "udfs/citus_drop_trigger/9.5-1.sql"
#include "udfs/worker_record_sequence_dependency/9.5-1.sql"
#include "udfs/citus_finish_pg_upgrade/9.5-1.sql"
#include "udfs/citus_prepare_pg_upgrade/9.5-1.sql"

SET search_path = 'pg_catalog';

DROP FUNCTION task_tracker_assign_task(bigint, integer, text);
DROP FUNCTION task_tracker_task_status(bigint, integer);
DROP FUNCTION task_tracker_cleanup_job(bigint);
DROP FUNCTION worker_merge_files_and_run_query(bigint, integer, text, text);
DROP FUNCTION worker_execute_sql_task(bigint, integer, text, bool);
DROP TRIGGER dist_authinfo_task_tracker_cache_invalidate ON pg_catalog.pg_dist_authinfo;
DROP TRIGGER dist_poolinfo_task_tracker_cache_invalidate ON pg_catalog.pg_dist_poolinfo;
DROP FUNCTION task_tracker_conninfo_cache_invalidate();
DROP FUNCTION master_drop_sequences(text[]);

CREATE FUNCTION pg_catalog.partial_agg_sfunc(internal, oid, anyelement, int)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_catalog.partial_agg_ffunc(internal)
RETURNS bytea
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE AGGREGATE pg_catalog.partial_agg(oid, anyelement, int) (
    STYPE = internal,
    SFUNC = pg_catalog.partial_agg_sfunc,
    FINALFUNC = pg_catalog.partial_agg_ffunc
);

CREATE FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea, anyelement)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE FUNCTION pg_catalog.combine_agg_ffunc(internal, oid, bytea, anyelement)
RETURNS anyelement
AS 'MODULE_PATHNAME'
LANGUAGE C PARALLEL SAFE;

CREATE AGGREGATE pg_catalog.combine_agg(oid, bytea) (
    STYPE = internal,
    SFUNC = pg_catalog.combine_agg_sfunc,
    FINALFUNC = pg_catalog.partial_agg_ffunc

);

CREATE AGGREGATE pg_catalog.finalize_agg(oid, bytea, anyelement) (
    STYPE = internal,
    SFUNC = pg_catalog.combine_agg_sfunc,
    FINALFUNC = pg_catalog.combine_agg_ffunc,
    FINALFUNC_EXTRA
);

REVOKE ALL ON FUNCTION pg_catalog.partial_agg_sfunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.partial_agg_ffunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.partial_agg FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea, anyelement) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.combine_agg_ffunc FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.combine_agg FROM PUBLIC;
REVOKE ALL ON FUNCTION pg_catalog.finalize_agg FROM PUBLIC;

GRANT EXECUTE ON FUNCTION pg_catalog.partial_agg_sfunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.partial_agg_ffunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.partial_agg TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea, anyelement) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.combine_agg_sfunc(internal, oid, bytea) TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.combine_agg_ffunc TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.combine_agg TO PUBLIC;
GRANT EXECUTE ON FUNCTION pg_catalog.finalize_agg TO PUBLIC;

--  add pg_cimv TODO: finalize name of this table
CREATE TABLE citus.pg_cimv(
    userview regclass NOT NULL PRIMARY KEY,
    basetable regclass NOT NULL,
    mattable regclass NOT NULL,
    refreshview regclass NOT NULL,
    deltatriggerfnnamespace name NOT NULL,
    deltatriggerfnname name NOT NULL,
    landingtable regclass NOT NULL,
    jobid bigint NOT NULL
);

ALTER TABLE citus.pg_cimv SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_cimv TO public;

RESET search_path;
