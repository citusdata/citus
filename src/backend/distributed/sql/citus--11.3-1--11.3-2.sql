DROP VIEW citus_shards;
DROP VIEW IF EXISTS pg_catalog.citus_tables;
DROP VIEW IF EXISTS public.citus_tables;
DROP FUNCTION citus_shard_sizes;

#include "udfs/citus_shard_sizes/11.3-2.sql"

#include "udfs/citus_shards/11.3-2.sql"
#include "udfs/citus_tables/11.3-2.sql"
