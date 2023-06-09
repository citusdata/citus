-- citus--11.3-1--12.0-1

-- bump version to 12.0-1

CREATE TABLE citus.pg_dist_tenant_schema (
    schemaid oid NOT NULL,
    colocationid int NOT NULL,
    CONSTRAINT pg_dist_tenant_schema_pkey PRIMARY KEY (schemaid),
    CONSTRAINT pg_dist_tenant_schema_unique_colocationid_index UNIQUE (colocationid)
);

ALTER TABLE citus.pg_dist_tenant_schema SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.pg_dist_tenant_schema TO public;

-- udfs used to modify pg_dist_tenant_schema on workers, to sync metadata
#include "udfs/citus_internal_add_tenant_schema/12.0-1.sql"
#include "udfs/citus_internal_delete_tenant_schema/12.0-1.sql"

#include "udfs/citus_prepare_pg_upgrade/12.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/12.0-1.sql"

-- udfs used to modify pg_dist_tenant_schema globally via drop trigger
#include "udfs/citus_internal_unregister_tenant_schema_globally/12.0-1.sql"
#include "udfs/citus_drop_trigger/12.0-1.sql"

#include "udfs/citus_tables/12.0-1.sql"
#include "udfs/citus_shards/12.0-1.sql"
