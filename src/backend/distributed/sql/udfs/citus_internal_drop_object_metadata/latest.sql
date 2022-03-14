CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_drop_object_metadata(
							typeText text,
                            objNames text[],
                            objArgs text[])
    RETURNS void
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_drop_object_metadata(text,text[],text[]) IS
    'Drops distributed object from pg_dist_object';
