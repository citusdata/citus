CREATE OR REPLACE PROCEDURE pg_catalog.citus_cleanup_orphaned_resources()
    LANGUAGE C
    AS 'citus', $$citus_cleanup_orphaned_resources$$;
COMMENT ON PROCEDURE pg_catalog.citus_cleanup_orphaned_resources()
    IS 'cleanup orphaned resources';
