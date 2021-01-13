CREATE OR REPLACE FUNCTION pg_catalog.citus_dist_node_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$citus_dist_node_cache_invalidate$$;
COMMENT ON FUNCTION pg_catalog.citus_dist_node_cache_invalidate()
    IS 'register relcache invalidation for changed rows';
