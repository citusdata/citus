/* citus--7.5-6--7.5-7 */
SET search_path = 'pg_catalog';

CREATE FUNCTION pg_catalog.poolinfo_valid(text)
	RETURNS boolean
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$poolinfo_valid$$;
COMMENT ON FUNCTION pg_catalog.poolinfo_valid(text) IS 'returns whether a poolinfo is valid';

CREATE TABLE citus.pg_dist_poolinfo (
    nodeid integer PRIMARY KEY
                   REFERENCES pg_dist_node(nodeid)
                              ON DELETE CASCADE,
    poolinfo text NOT NULL
	              CONSTRAINT poolinfo_valid
								CHECK (poolinfo_valid(poolinfo))
);

ALTER TABLE citus.pg_dist_poolinfo SET SCHEMA pg_catalog;
GRANT SELECT ON pg_catalog.pg_dist_poolinfo TO public;

ALTER FUNCTION master_dist_authinfo_cache_invalidate()
RENAME TO master_conninfo_cache_invalidate;

CREATE TRIGGER dist_poolinfo_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_poolinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE master_conninfo_cache_invalidate();

CREATE TRIGGER dist_poolinfo_task_tracker_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE
    ON pg_catalog.pg_dist_poolinfo
    FOR EACH STATEMENT EXECUTE PROCEDURE task_tracker_conninfo_cache_invalidate();

RESET search_path;
