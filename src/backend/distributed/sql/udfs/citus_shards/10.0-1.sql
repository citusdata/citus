CREATE OR REPLACE VIEW citus.citus_shards AS
WITH shard_sizes AS (SELECT * FROM pg_catalog.citus_shard_sizes())
SELECT
     pg_dist_shard.logicalrelid AS table_name,
     pg_dist_shard.shardid,
     shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) as shard_name,
     CASE WHEN partkey IS NOT NULL THEN 'distributed' WHEN repmodel = 't' THEN 'reference' ELSE 'local' END AS citus_table_type,
     colocationid AS colocation_id,
     pg_dist_node.nodename,
     pg_dist_node.nodeport,
     (SELECT size FROM shard_sizes WHERE
       shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) = table_name
       OR
       'public.' || shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) = table_name
      LIMIT 1) as shard_size
FROM
   pg_dist_shard
JOIN
   pg_dist_placement
ON
   pg_dist_shard.shardid = pg_dist_placement.shardid
JOIN
   pg_dist_node
ON
   pg_dist_placement.groupid = pg_dist_node.groupid
JOIN
   pg_dist_partition
ON
   pg_dist_partition.logicalrelid = pg_dist_shard.logicalrelid
ORDER BY
   pg_dist_shard.logicalrelid::text, shardid
;

ALTER VIEW citus.citus_shards SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_shards TO public;
