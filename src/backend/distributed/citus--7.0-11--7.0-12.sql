/* citus--7.0-11--7.0-12.sql */

CREATE OR REPLACE FUNCTION pg_catalog.citus_create_restore_point(text)
RETURNS pg_lsn
LANGUAGE C STRICT
AS 'MODULE_PATHNAME', $$citus_create_restore_point$$;
COMMENT ON FUNCTION pg_catalog.citus_create_restore_point(text)
IS 'temporarily block writes and create a named restore point on all nodes';
