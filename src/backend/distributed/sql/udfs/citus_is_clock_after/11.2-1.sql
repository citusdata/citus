CREATE OR REPLACE FUNCTION pg_catalog.citus_is_clock_after(clock_one pg_catalog.cluster_clock, clock_two pg_catalog.cluster_clock)
    RETURNS BOOL
    LANGUAGE C STABLE PARALLEL SAFE STRICT
    AS 'MODULE_PATHNAME',$$citus_is_clock_after$$;

COMMENT ON FUNCTION pg_catalog.citus_is_clock_after(pg_catalog.cluster_clock, pg_catalog.cluster_clock)
    IS 'Accepts logical clock timestamps of two causally related events and returns true if the argument1 happened before argument2';

