CREATE OR REPLACE FUNCTION pg_catalog.citus_rebalance_wait()
    RETURNS VOID
    AS 'MODULE_PATHNAME'
    LANGUAGE C VOLATILE;
COMMENT ON FUNCTION pg_catalog.citus_rebalance_wait()
    IS 'wait on a running rebalance in the background';
GRANT EXECUTE ON FUNCTION pg_catalog.citus_rebalance_wait() TO PUBLIC;
