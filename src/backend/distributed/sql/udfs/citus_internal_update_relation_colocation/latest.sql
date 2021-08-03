CREATE OR REPLACE FUNCTION pg_catalog.citus_internal_update_relation_colocation(relation_id Oid, target_colocation_id int)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME';
COMMENT ON FUNCTION pg_catalog.citus_internal_update_relation_colocation(oid, int) IS
    'Updates colocationId field of pg_dist_partition for the relation_id';

