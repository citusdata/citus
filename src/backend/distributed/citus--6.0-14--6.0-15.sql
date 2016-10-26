/* citus--6.0-14--6.0-15.sql */


CREATE FUNCTION pg_catalog.master_dist_placement_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_placement_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_placement_cache_invalidate()
    IS 'register relcache invalidation for changed placements';

CREATE TRIGGER dist_placement_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_shard_placement
    FOR EACH ROW EXECUTE PROCEDURE master_dist_placement_cache_invalidate();
