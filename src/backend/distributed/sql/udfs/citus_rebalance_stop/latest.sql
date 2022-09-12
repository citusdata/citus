CREATE OR REPLACE FUNCTION pg_catalog.citus_rebalance_stop()
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_rebalance_stop()
    IS 'stop a rebalance that is running in the background';
GRANT EXECUTE ON FUNCTION pg_catalog.citus_rebalance_stop() TO PUBLIC;
