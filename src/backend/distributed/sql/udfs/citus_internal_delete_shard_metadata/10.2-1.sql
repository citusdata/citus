CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_delete_shard_metadata(shard_id bigint)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_delete_shard_metadata(bigint) IS
    'Deletes rows from pg_dist_shard and pg_dist_shard_placement with user checks';

