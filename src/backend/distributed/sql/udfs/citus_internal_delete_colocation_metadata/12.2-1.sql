CREATE OR REPLACE FUNCTION citus_internal.delete_colocation_metadata(
							colocation_id int)
    RETURNS void
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_delete_colocation_metadata$$;

COMMENT ON FUNCTION citus_internal.delete_colocation_metadata(int) IS
    'deletes a co-location group from pg_dist_colocation';

CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_colocation_metadata(
							colocation_id int)
    RETURNS void
    LANGUAGE C
    STRICT
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_colocation_metadata(int) IS
    'deletes a co-location group from pg_dist_colocation';
