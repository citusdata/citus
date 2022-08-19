CREATE FUNCTION pg_catalog.citus_jobs_wait(jobid bigint)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$citus_jobs_wait$$;
COMMENT ON FUNCTION pg_catalog.citus_jobs_wait(jobid bigint)
    IS 'blocks till the job identified by jobid is at a terminal state, errors if no such job exists';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_jobs_wait(jobid bigint) TO PUBLIC;
