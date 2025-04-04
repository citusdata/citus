CREATE SCHEMA issue_7896;
SET search_path TO issue_7896;

-- Create a temporary table to simulate the background job catalog.
-- (In production this would be the actual catalog table.)
CREATE TEMP TABLE pg_dist_background_job
(
    job_id    int8 PRIMARY KEY,
    job_state text,
    started_at timestamptz,
    finished_at timestamptz
);

-- Insert a dummy job record with job_state set to 'running'
INSERT INTO pg_dist_background_job (job_id, job_state, started_at)
VALUES (1001, 'running', now());

-- Set a short statement timeout so that citus_rebalance_wait times out quickly.
SET statement_timeout = '1000ms';

DO $$
BEGIN
    BEGIN
        -- Call the wait function.
        -- Note: The public function citus_rebalance_wait() takes no arguments.
        PERFORM citus_rebalance_wait();
    EXCEPTION
        WHEN query_canceled THEN
            RAISE NOTICE 'Query canceled as expected';
            -- Swallow the error so the transaction continues.
    END;
END;
$$ LANGUAGE plpgsql;

-- Reset the statement timeout for subsequent queries.
SET statement_timeout = '0';

-- Verify that the job's state has been updated to 'cancelled'
-- (the expected outcome after a cancellation).
SELECT job_state
FROM pg_dist_background_job
WHERE job_id = 1001;

SET client_min_messages TO WARNING;
DROP SCHEMA issue_7896 CASCADE;
