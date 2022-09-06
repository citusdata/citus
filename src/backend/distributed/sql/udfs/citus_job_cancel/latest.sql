CREATE FUNCTION pg_catalog.citus_job_cancel(jobid bigint)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME',$$citus_job_cancel$$;
COMMENT ON FUNCTION pg_catalog.citus_job_cancel(jobid bigint)
    IS 'cancel a scheduled or running job and all of its tasks that didn''t finish yet';

GRANT EXECUTE ON FUNCTION pg_catalog.citus_job_cancel(jobid bigint) TO PUBLIC;
