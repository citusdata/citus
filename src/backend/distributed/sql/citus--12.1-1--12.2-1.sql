-- citus--12.1-1--12.2-1
-- bump version to 12.2-1

#include "udfs/citus_internal_database_command/12.2-1.sql"
#include "udfs/citus_add_rebalance_strategy/12.2-1.sql"

CREATE TABLE citus.pg_dist_database (
    databaseid oid NOT NULL,
    groupid integer NOT NULL,
    CONSTRAINT pg_dist_database_pkey PRIMARY KEY (databaseid)
);

ALTER TABLE citus.pg_dist_database SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_database TO public;

#include "udfs/citus_internal_pg_database_size_local/12.2-1.sql"
-- #include "udfs/citus_pg_dist_database/12.2-1.sql"
