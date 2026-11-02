CREATE OR REPLACE FUNCTION citus_internal.lock_colocation_id(colocation_id int, lock_mode int)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_lock_colocation_id$$;
COMMENT ON FUNCTION citus_internal.lock_colocation_id(int, int)
    IS 'acquire a lock on a colocation id';
