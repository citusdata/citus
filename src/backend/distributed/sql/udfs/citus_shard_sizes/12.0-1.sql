CREATE OR REPLACE FUNCTION pg_catalog.citus_shard_sizes(OUT shard_id int, OUT size bigint)
  RETURNS SETOF RECORD
  LANGUAGE C STRICT
  AS 'MODULE_PATHNAME', $$citus_shard_sizes$$;
 COMMENT ON FUNCTION pg_catalog.citus_shard_sizes(OUT shard_id int, OUT size bigint)
     IS 'returns shards sizes across citus cluster';
