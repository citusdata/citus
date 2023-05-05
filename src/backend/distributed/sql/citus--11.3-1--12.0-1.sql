-- citus--11.3-1--12.0-1

-- bump version to 12.0-1

CREATE TABLE citus.pg_dist_tenant_schema (
    schema_id oid NOT NULL,
    colocation_id int NOT NULL,
    CONSTRAINT pg_dist_tenant_schema_pkey PRIMARY KEY (schema_id),
    CONSTRAINT pg_dist_tenant_schema_unique_colocationid_index UNIQUE (colocation_id)
);

ALTER TABLE citus.pg_dist_tenant_schema SET SCHEMA pg_catalog;

GRANT SELECT ON pg_catalog.pg_dist_tenant_schema TO public;

#include "udfs/citus_internal_add_tenant_schema/12.0-1.sql"
#include "udfs/citus_internal_delete_tenant_schema/12.0-1.sql"

#include "udfs/citus_prepare_pg_upgrade/12.0-1.sql"
#include "udfs/citus_finish_pg_upgrade/12.0-1.sql"
