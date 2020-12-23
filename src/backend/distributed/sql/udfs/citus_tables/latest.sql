CREATE VIEW public.citus_tables AS
SELECT
  logicalrelid AS "Name",
  CASE WHEN partkey IS NOT NULL THEN 'distributed' ELSE 'reference' END AS "Citus Table Type",
  coalesce(column_to_column_name(logicalrelid, partkey), '<none>') AS "Distribution Column",
  colocationid AS "Colocation ID",
  pg_size_pretty(citus_total_relation_size(logicalrelid, fail_on_error := false)) AS "Size",
  (select count(*) from pg_dist_shard where logicalrelid = p.logicalrelid) AS "Shard Count",
  pg_get_userbyid(relowner) AS "Owner",
  amname AS "Access Method"
FROM
  pg_dist_partition p
JOIN
  pg_class c ON (p.logicalrelid = c.oid)
LEFT JOIN
  pg_am a ON (a.oid = c.relam)
WHERE
  partkey IS NOT NULL OR repmodel = 't'
ORDER BY
  logicalrelid::text;
