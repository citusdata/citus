-- citus--11.2-1--11.3-1

-- bump version to 11.3-1

CREATE TABLE citus.pg_dist_shardgroup (
    shardgroupid bigint PRIMARY KEY,
    colocationid integer NOT NULL,
    shardminvalue text,
    shardmaxvalue text
);
ALTER TABLE citus.pg_dist_shardgroup SET SCHEMA pg_catalog;

INSERT INTO pg_catalog.pg_dist_shardgroup
     SELECT min(shardid) as shardgroupid,
            colocationid,
            shardminvalue,
            shardmaxvalue
       FROM pg_dist_shard
       JOIN pg_dist_partition USING (logicalrelid)
   GROUP BY colocationid, shardminvalue, shardmaxvalue;

ALTER TABLE pg_catalog.pg_dist_shard ADD COLUMN shardgroupid bigint;

-- backfill shardgroupid field by finding the generated shardgroup above by joining the colocationid, shardminvalue and
-- shardmaxvalue (for the shardvalues we want to treat NULL values as equal, hence the complex conditions for those).
-- After this operation _all_ shards should have a shardgroupid associated which satisfies the colocation invariant of
-- the shards in the same colocationid.
UPDATE pg_catalog.pg_dist_shard AS shard
   SET shardgroupid = shardgroup.shardgroupid
  FROM (
      SELECT shardgroupid,
             colocationid,
             shardminvalue,
             shardmaxvalue,
             logicalrelid
        FROM pg_catalog.pg_dist_shardgroup
        JOIN pg_dist_partition USING (colocationid)
  ) AS shardgroup
WHERE shard.logicalrelid = shardgroup.logicalrelid
  AND (
       shard.shardminvalue = shardgroup.shardminvalue
       OR (    shard.shardminvalue      IS NULL
           AND shardgroup.shardminvalue IS NULL)
  )
  AND (
       shard.shardmaxvalue = shardgroup.shardmaxvalue
       OR (    shard.shardmaxvalue      IS NULL
           AND shardgroup.shardmaxvalue IS NULL)
  );

-- risky, but we want to fail quickly here. If there are cases where a shard is not associated correctly with a
-- shardgroup we would not want the setup to use the new Citus version as it hard relies on the shardgroups being
-- correctly associated.
ALTER TABLE pg_catalog.pg_dist_shard ALTER COLUMN shardgroupid SET NOT NULL;

#include "udfs/citus_internal_add_shard_metadata/11.3-1.sql"
DROP FUNCTION pg_catalog.citus_internal_add_shard_metadata(
    relation_id regclass, shard_id bigint,
    storage_type "char", shard_min_value text,
    shard_max_value text
);

#include "udfs/citus_internal_add_shardgroup_metadata/11.3-1.sql"
