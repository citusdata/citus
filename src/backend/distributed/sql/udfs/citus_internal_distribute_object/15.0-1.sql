CREATE OR REPLACE FUNCTION citus_internal.distribute_object(classid oid, objid oid)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_distribute_object$$;
COMMENT ON FUNCTION citus_internal.distribute_object(oid, oid)
    IS 'recreate the given object on all worker nodes and record it in pg_dist_object on all nodes';
REVOKE ALL ON FUNCTION citus_internal.distribute_object(oid, oid) FROM PUBLIC;
