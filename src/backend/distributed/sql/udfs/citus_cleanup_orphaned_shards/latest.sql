CREATE OR REPLACE PROCEDURE pg_catalog.citus_cleanup_orphaned_shards()
    LANGUAGE C
    AS 'citus', $$citus_cleanup_orphaned_shards$$;
COMMENT ON PROCEDURE pg_catalog.citus_cleanup_orphaned_shards()
    IS 'cleanup orphaned shards';
