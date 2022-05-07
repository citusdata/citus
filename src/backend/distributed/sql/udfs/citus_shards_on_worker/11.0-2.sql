CREATE OR REPLACE FUNCTION pg_catalog.citus_shards_on_worker(
     OUT schema_name name,
     OUT shard_name name,
     OUT table_type text,
     OUT owner_name name)
 RETURNS SETOF record
 LANGUAGE plpgsql
 SET citus.show_shards_for_app_name_prefixes = '*'
 AS $$
BEGIN
  -- this is the query that \d produces, except pg_table_is_visible
  -- is replaced with pg_catalog.relation_is_a_known_shard(c.oid)
  RETURN QUERY
	SELECT n.nspname as "Schema",
	  c.relname as "Name",
	  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 's' THEN 'special' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'table' END as "Type",
	  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
	FROM pg_catalog.pg_class c
	     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
	WHERE c.relkind IN ('r','p','v','m','S','f','')
	      AND n.nspname <> 'pg_catalog'
	      AND n.nspname <> 'information_schema'
	      AND n.nspname !~ '^pg_toast'
  		  AND pg_catalog.relation_is_a_known_shard(c.oid)
	ORDER BY 1,2;
END;
$$;

CREATE OR REPLACE VIEW pg_catalog.citus_shards_on_worker AS
	SELECT schema_name as "Schema",
	  shard_name as "Name",
	  table_type as "Type",
	  owner_name as "Owner"
	FROM pg_catalog.citus_shards_on_worker() s;
