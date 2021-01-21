-- citus--9.5-1--10.0-1

DROP FUNCTION pg_catalog.upgrade_to_reference_table(regclass);
DROP FUNCTION IF EXISTS pg_catalog.citus_total_relation_size(regclass);

#include "udfs/citus_total_relation_size/10.0-1.sql"
#include "udfs/citus_tables/10.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-1.sql"
#include "udfs/citus_unique_id/10.0-1.sql"

#include "../../columnar/sql/columnar--9.5-1--10.0-1.sql"

CREATE SCHEMA cimv_internal;
GRANT ALL ON SCHEMA cimv_internal to public;

CREATE FUNCTION cimv_internal.cimv_trigger()
    RETURNS trigger
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cimv_trigger$$;

CREATE FUNCTION pg_catalog.worker_record_trigger_dependency(basetable_name regclass, inserttable_name regclass, trigger_name text[])
  RETURNS VOID
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', 'worker_record_trigger_dependency';
COMMENT ON FUNCTION pg_catalog.worker_record_trigger_dependency(regclass,regclass,text[])
  IS 'record the fact that the trigger depends on the table in pg_depend';
   