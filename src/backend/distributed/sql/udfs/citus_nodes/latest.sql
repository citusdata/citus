CREATE VIEW citus.citus_nodes AS
SELECT
  nodename AS "Host",
  nodeport AS "Port",
  CASE WHEN groupid = 0 THEN 'coordinator' ELSE 'worker' END AS "Node Type",
  pg_size_pretty(citus_node_database_size(nodename, nodeport)) AS "Database Size",
  (SELECT
     count(*)
   FROM
     pg_dist_partition t
   JOIN
     pg_dist_shard s
   USING
     (logicalrelid)
   JOIN
     pg_dist_placement p
   USING
     (shardid)
   WHERE p.groupid = n.groupid AND t.partmethod <> 'n'
  ) AS "Distributed Table Shards",
  (SELECT
     count(*)
   FROM
     pg_dist_partition t
   JOIN
     pg_dist_shard s
   USING
     (logicalrelid)
   JOIN
     pg_dist_placement p
   USING
     (shardid)
   WHERE p.groupid = n.groupid AND t.partmethod = 'n' AND t.repmodel = 't'
  ) AS "Reference Tables",
  round(100 * (1. - (ds.available_disk_size::double precision / ds.total_disk_size))) || '%' AS "Disk Usage"
FROM
  pg_dist_node n,
  citus_node_disk_space_stats(n.nodename, n.nodeport) ds
ORDER BY
  groupid;

ALTER VIEW citus.citus_nodes SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.citus_nodes TO public;
