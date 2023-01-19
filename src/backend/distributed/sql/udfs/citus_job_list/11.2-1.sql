CREATE OR REPLACE FUNCTION pg_catalog.citus_job_list ()
    RETURNS TABLE (
            job_id bigint,
            state pg_catalog.citus_job_status,
            job_type name,
            description text,
            started_at timestamptz,
            finished_at timestamptz
)
    LANGUAGE SQL
    AS $fn$
    SELECT
        job_id,
        state,
        job_type,
        description,
        started_at,
        finished_at
    FROM
        pg_dist_background_job
    ORDER BY
        job_id
$fn$;
