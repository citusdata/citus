CREATE OR REPLACE FUNCTION citus_internal.acquire_placement_colocation_lock(relation_id regclass, lock_mode int)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_acquire_placement_colocation_lock$$;
COMMENT ON FUNCTION citus_internal.acquire_placement_colocation_lock(regclass, int)
    IS 'acquire a placement colocation lock on a colocation id';
