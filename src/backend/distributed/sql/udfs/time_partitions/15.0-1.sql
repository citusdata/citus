CREATE OR REPLACE VIEW pg_catalog.time_partitions AS
SELECT partrelid AS parent_table, attname AS partition_column, relid AS partition, lower_bound AS from_value, upper_bound AS to_value, amname AS access_method
FROM (
  SELECT partrelid::regclass AS partrelid, attname, c.oid::regclass AS relid, lower_bound, upper_bound, amname
  FROM pg_class c
  JOIN pg_inherits i ON (c.oid = inhrelid)
  JOIN pg_partitioned_table p ON (inhparent = partrelid)
  JOIN pg_attribute a ON (partrelid = attrelid)
  JOIN pg_type t ON (atttypid = t.oid)
  JOIN pg_namespace tn ON (t.typnamespace = tn.oid)
  LEFT JOIN pg_am am ON (c.relam = am.oid),
  -- The function may run before the joins, so check the child's parent and
  -- its partition strategy inside the STRICT function's argument.
  pg_catalog.time_partition_range(
    CASE WHEN i.inhparent = p.partrelid AND p.partstrat = 'r' AND p.partnatts = 1
         THEN i.inhrelid END)
  WHERE c.relpartbound IS NOT NULL AND p.partstrat = 'r' AND p.partnatts = 1
  AND a.attnum = ANY(partattrs::int2[])
) partitions
ORDER BY partrelid::text, lower_bound;

GRANT SELECT ON pg_catalog.time_partitions TO public;
