/* citus--6.1-14--6.1-15.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION master_dist_local_group_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_local_group_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_local_group_cache_invalidate()
    IS 'register node cache invalidation for changed rows';
    
CREATE TRIGGER dist_local_group_cache_invalidate
    AFTER UPDATE
    ON pg_catalog.pg_dist_local_group
    FOR EACH ROW EXECUTE PROCEDURE master_dist_local_group_cache_invalidate();
    
RESET search_path;
