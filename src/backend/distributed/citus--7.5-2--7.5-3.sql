/* citus--7.5-2--7.5-3 */
SET search_path = 'pg_catalog';

CREATE FUNCTION master_dist_authinfo_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'citus', $$master_dist_authinfo_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_authinfo_cache_invalidate()
    IS 'register authinfo cache invalidation on any modifications';

CREATE FUNCTION task_tracker_conninfo_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'citus', $$task_tracker_conninfo_cache_invalidate$$;
COMMENT ON FUNCTION task_tracker_conninfo_cache_invalidate()
    IS 'invalidate task-tracker conninfo cache';

CREATE TRIGGER dist_authinfo_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_authinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE master_dist_authinfo_cache_invalidate();

CREATE TRIGGER dist_authinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_authinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

RESET search_path;
