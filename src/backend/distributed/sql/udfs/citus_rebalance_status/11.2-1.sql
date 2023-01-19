CREATE OR REPLACE FUNCTION pg_catalog.citus_rebalance_status (
    raw boolean DEFAULT FALSE
)
    RETURNS TABLE (
            job_id bigint,
            state pg_catalog.citus_job_status,
            job_type name,
            description text,
            started_at timestamptz,
            finished_at timestamptz,
            details jsonb
)
    LANGUAGE SQL
    STRICT
    AS $fn$
    SELECT
        job_status.*
    FROM
        pg_dist_background_job j,
        citus_job_status (j.job_id, $1) job_status
    WHERE
        j.job_id IN (
            SELECT job_id
            FROM pg_dist_background_job
            WHERE job_type = 'rebalance'
            ORDER BY job_id DESC
            LIMIT 1
        );
$fn$;
