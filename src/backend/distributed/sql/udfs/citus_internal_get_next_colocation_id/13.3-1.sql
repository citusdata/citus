CREATE OR REPLACE FUNCTION citus_internal.get_next_colocation_id()
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_internal_get_next_colocation_id$$;
COMMENT ON FUNCTION citus_internal.get_next_colocation_id()
    IS 'retrieves the next colocation id from pg_dist_colocationid_seq';
