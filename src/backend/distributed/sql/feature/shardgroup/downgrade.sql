DROP TABLE pg_dist_shardgroup;

-- TODO probably needs to drop table and recreate original one to make sure we can upgrade
-- again later and _not_ have troubled with the internal ordering of the columns.
ALTER TABLE pg_catalog.pg_dist_shard DROP COLUMN shardgroupid;

DROP FUNCTION pg_catalog.citus_internal_add_shardgroup_metadata(bigint, integer);
DROP FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(bigint);

DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, bigint);
#include "../../udfs/citus_internal_add_shard_metadata/10.2-1.sql"
