-- citus--11.3-1--11.2-1
-- this is an empty downgrade path since citus--11.2-1--11.3-1.sql is empty for now
DROP TABLE pg_catalog.pg_dist_shardgroup;
DROP SEQUENCE citus.pg_dist_shardgroupid_seq;

ALTER TABLE pg_catalog.pg_dist_shard DROP COLUMN shardgroupid;

DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text, bigint);
#include "../udfs/citus_internal_add_shard_metadata/10.2-1.sql"

DROP FUNCTION pg_catalog.citus_internal_add_shardgroup_metadata(bigint, integer, text, text);
DROP FUNCTION FUNCTION pg_catalog.citus_internal_delete_shardgroup_metadata(bigint, int);
