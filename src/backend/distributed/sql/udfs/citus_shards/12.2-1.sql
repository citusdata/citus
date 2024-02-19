CREATE OR REPLACE VIEW citus.citus_shards AS
SELECT
     pg_dist_shard.logicalrelid AS table_name,
     pg_dist_shard.shardid,
     shard_name(pg_dist_shard.logicalrelid, pg_dist_shard.shardid) as shard_name,
     CASE WHEN colocationid IN (SELECT colocationid FROM pg_dist_schema) THEN 'schema'
      WHEN partkey IS NOT NULL THEN 'distributed'
      WHEN repmodel = 't' THEN 'reference'
      WHEN colocationid = 0 THEN 'local'
      ELSE 'distributed' END AS citus_table_type,
     colocationid AS colocation_id,
     pg_dist_node.nodename,
     pg_dist_node.nodeport,
     size as shard_size,
     CASE
      WHEN NOT pg_dist_shard.needsseparatenode THEN false
      ELSE
        -- has_separate_node = true if the node doesn't have any other shard
        -- placements except the ones that belong to the same shard group.
        NOT EXISTS (
            SELECT 1
            FROM pg_dist_shard pds1
            JOIN pg_dist_placement pdp1 USING (shardid)
            JOIN pg_dist_partition pdp2 USING (logicalrelid)
            WHERE
                  -- get the distributed table placements that are placed on the same node as this placement
                  pdp1.groupid = pg_dist_placement.groupid AND
                  (pdp2.partkey IS NOT NULL OR (pdp2.repmodel != 't' AND pdp2.colocationid != 0)) AND
                  -- filter out all the placements that belong to the same shard group
                  NOT (
                    pdp2.colocationid = pg_dist_partition.colocationid AND
                    ((pds1.shardminvalue IS NULL AND pg_dist_shard.shardminvalue IS NULL) OR (pds1.shardminvalue = pg_dist_shard.shardminvalue))
                  )
        )
     END AS has_separate_node
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
LEFT JOIN
   (SELECT shard_id, max(size) as size from citus_shard_sizes() GROUP BY shard_id) as shard_sizes
ON
    pg_dist_shard.shardid = shard_sizes.shard_id
WHERE
   pg_dist_placement.shardstate = 1
AND
   -- filter out tables owned by extensions
   pg_dist_partition.logicalrelid NOT IN (
      SELECT
         objid
      FROM
         pg_depend
      WHERE
         classid = 'pg_class'::regclass AND refclassid = 'pg_extension'::regclass AND deptype = 'e'
   )
ORDER BY
   pg_dist_shard.logicalrelid::text, shardid
;

ALTER VIEW citus.citus_shards SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_shards TO public;
