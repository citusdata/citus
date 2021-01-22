CREATE VIEW public.citus_tables AS
SELECT
  logicalrelid AS table_name,
  CASE WHEN partkey IS NOT NULL THEN 'distributed' ELSE 'reference' END AS citus_table_type,
  coalesce(column_to_column_name(logicalrelid, partkey), '<none>') AS distribution_column,
  colocationid AS colocation_id,
  pg_size_pretty(citus_total_relation_size(logicalrelid, fail_on_error := false)) AS table_size,
  (select count(*) from pg_dist_shard where logicalrelid = p.logicalrelid) AS shard_count,
  pg_get_userbyid(relowner) AS table_owner,
  amname AS access_method
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
