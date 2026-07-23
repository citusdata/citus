CREATE OR REPLACE FUNCTION citus_internal.distribute_object(classid oid, objid oid, objsubid int DEFAULT 0, force_recreate boolean DEFAULT false)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_distribute_object$$;
COMMENT ON FUNCTION citus_internal.distribute_object(oid, oid, int, boolean)
    IS 'recreate the given object (but not its dependencies) on all worker nodes and record it in pg_dist_object on all nodes; the worker DDL and the pg_dist_object writes are not performed atomically';
REVOKE ALL ON FUNCTION citus_internal.distribute_object(oid, oid, int, boolean) FROM PUBLIC;
