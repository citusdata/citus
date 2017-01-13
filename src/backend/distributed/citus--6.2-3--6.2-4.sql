/* citus--6.2-3--6.2-4.sql */

CREATE OR REPLACE FUNCTION pg_catalog.citus_truncate_trigger()
    RETURNS trigger
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_truncate_trigger$$;
COMMENT ON FUNCTION pg_catalog.citus_truncate_trigger()
    IS 'trigger function called when truncating the distributed table';
