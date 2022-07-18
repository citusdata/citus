CREATE FUNCTION pg_catalog.citus_wait_for_rebalance_job(jobid bigint)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$citus_wait_for_rebalance_job$$;
COMMENT ON FUNCTION pg_catalog.citus_wait_for_rebalance_job(jobid bigint)
    IS 'blocks till the job identified by jobid is at a terminal state, errors if no such job exists';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_wait_for_rebalance_job(jobid bigint) TO PUBLIC;
