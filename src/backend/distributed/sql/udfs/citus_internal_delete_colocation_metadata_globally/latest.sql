CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_colocation_metadata_globally(
						   colocation_id int)
    RETURNS void
    LANGUAGE C
    VOLATILE
    AS 'MODULE_PATHNAME';

COMMENT ON FUNCTION pg_catalog.citus_internal_delete_colocation_metadata_globally(int) IS
    'deletes a co-location group from pg_dist_colocation globally';
