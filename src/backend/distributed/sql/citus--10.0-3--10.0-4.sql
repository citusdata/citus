--
-- 10.0-3--10.0-4 was added later as a patch to fix a bug in our PG upgrade functions
--
-- Users who took the 9.5-2--10.0-1 upgrade path already have the fix, but users
-- who took the 9.5-1--10.0-1 upgrade path do not. Hence, we repeat the CREATE OR
-- REPLACE from the 9.5-2 definition for citus_prepare_pg_upgrade.

#include "udfs/citus_prepare_pg_upgrade/9.5-2.sql"
#include "udfs/citus_finish_pg_upgrade/10.0-4.sql"
