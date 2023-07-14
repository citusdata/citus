DROP VIEW IF EXISTS public.citus_tables;
DROP VIEW IF EXISTS pg_catalog.citus_tables;

DROP VIEW pg_catalog.citus_shards;
DROP FUNCTION pg_catalog.citus_shard_sizes;
#include "../udfs/citus_shard_sizes/10.0-1.sql"
-- citus_shards/11.1-1.sql tries to create citus_shards in pg_catalog but it is not allowed.
-- Here we use citus_shards/10.0-1.sql to properly create the view in citus schema and
-- then alter it to pg_catalog, so citus_shards/11.1-1.sql can REPLACE it without any errors.
#include "../udfs/citus_shards/10.0-1.sql"

#include "../udfs/citus_tables/11.1-1.sql"
#include "../udfs/citus_shards/11.1-1.sql"
