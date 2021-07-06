-- citus--10.0-3--10.0-4

-- This migration file aims to fix 2 issues with upgrades on clusters

-- 1. a bug in public schema dependency for citus_tables view.
--
-- Users who do not have public schema in their clusters were unable to upgrade
-- to Citus 10.x due to the citus_tables view that used to be created in public
-- schema

#include "udfs/citus_tables/10.0-4.sql"

-- 2. a bug in our PG upgrade functions
--
-- Users who took the 9.5-2--10.0-1 upgrade path already have the fix, but users
-- who took the 9.5-1--10.0-1 upgrade path do not. Hence, we repeat the CREATE OR
-- REPLACE from the 9.5-2 definition for citus_prepare_pg_upgrade.

#include "udfs/citus_prepare_pg_upgrade/9.5-2.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-4.sql"
