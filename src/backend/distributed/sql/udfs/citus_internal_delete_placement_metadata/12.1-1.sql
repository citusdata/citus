CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_placement_metadata(
    placement_id bigint)
RETURNS void
LANGUAGE C
VOLATILE
AS 'MODULE_PATHNAME',
$$citus_internal_delete_placement_metadata$$;
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_placement_metadata(bigint)
    IS 'Delete placement with given id from pg_dist_placement metadata table.';
