CREATE FUNCTION pg_catalog.citus_shard_sizes(OUT table_name text, OUT size bigint)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_shard_sizes$$;
 COMMENT ON FUNCTION pg_catalog.citus_shard_sizes(OUT table_name text, OUT size bigint)
     IS 'returns shards sizes across citus cluster';
