CREATE TABLE citus.pg_dist_shardgroup (
    shardgroupid bigint PRIMARY KEY,
    colocationid integer NOT NULL
);

ALTER TABLE citus.pg_dist_shardgroup SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.pg_dist_shard ADD COLUMN shardgroupid bigint NOT NULL;

CREATE SEQUENCE citus.pg_dist_shardgroupid_seq NO CYCLE;
ALTER SEQUENCE  citus.pg_dist_shardgroupid_seq SET SCHEMA pg_catalog;

DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(regclass, bigint, "char", text, text);
#include "../../udfs/citus_internal_add_shard_metadata/12.2-1.sql"
#include "../../udfs/citus_internal_add_shardgroup_metadata/12.2-1.sql"
#include "../../udfs/citus_internal_delete_shardgroup_metadata/12.2-1.sql"
