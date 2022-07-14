-- columnar--10.2-3--10.2-4.sql

#include "udfs/columnar_ensure_am_depends_catalog/10.2-4.sql"

PERFORM citus_internal.columnar_ensure_am_depends_catalog();
