CREATE OR REPLACE FUNCTION citus_internal.object_exists(object_type text, object_names text[], object_args text[])
    RETURNS boolean
    LANGUAGE plpgsql
    STABLE
    AS $function$
BEGIN
    -- Resolve the object by its textual identity. pg_get_object_address() raises
    -- when the object cannot be found, which we trap and report as "not found"
    -- so that probing existence over a transactional connection never aborts the
    -- surrounding transaction.
    PERFORM pg_catalog.pg_get_object_address(object_type, object_names, object_args);
    RETURN true;
EXCEPTION
    WHEN undefined_object OR undefined_function OR undefined_table OR
         undefined_column OR undefined_parameter OR invalid_schema_name OR
         wrong_object_type THEN
        RETURN false;
END;
$function$;
COMMENT ON FUNCTION citus_internal.object_exists(text, text[], text[])
    IS 'returns whether the object identified by its type/names/args exists on the local node, without raising if it does not';
REVOKE ALL ON FUNCTION citus_internal.object_exists(text, text[], text[]) FROM PUBLIC;

CREATE OR REPLACE FUNCTION citus_internal.distribute_object(classid oid, objid oid, force_recreate boolean DEFAULT false)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_distribute_object$$;
COMMENT ON FUNCTION citus_internal.distribute_object(oid, oid, boolean)
    IS 'recreate the given object on all worker nodes and record it in pg_dist_object on all nodes';
REVOKE ALL ON FUNCTION citus_internal.distribute_object(oid, oid, boolean) FROM PUBLIC;
