CREATE OR REPLACE FUNCTION pg_catalog.citus_get_node_clock()
    RETURNS pg_catalog.cluster_clock
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT
    AS 'MODULE_PATHNAME',$$citus_get_node_clock$$;
COMMENT ON FUNCTION pg_catalog.citus_get_node_clock()
    IS 'Returns monotonically increasing timestamp with logical clock value as close to epoch value (in milli seconds) as possible, and a counter for ticks(maximum of 4 million) within the logical clock';
