CREATE OR REPLACE FUNCTION pg_catalog.citus_job_status (
    job_id bigint,
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
    WITH rp AS MATERIALIZED (
        SELECT
            sessionid,
            sum(source_shard_size) as source_shard_size,
            sum(target_shard_size) as target_shard_size,
            any_value(status) as status,
            any_value(sourcename) as sourcename,
            any_value(sourceport) as sourceport,
            any_value(targetname) as targetname,
            any_value(targetport) as targetport,
            max(source_lsn) as source_lsn,
            min(target_lsn) as target_lsn
        FROM get_rebalance_progress()
        GROUP BY sessionid
    ),
    task_state_occurence_counts AS (
        SELECT t.status, count(task_id)
        FROM pg_dist_background_job j
            JOIN pg_dist_background_task t ON t.job_id = j.job_id
        WHERE j.job_id = $1
        GROUP BY t.status
    ),
    running_task_details AS (
        SELECT jsonb_agg(jsonb_build_object(
                'state', t.status,
                'retried', coalesce(t.retry_count,0),
                'phase', rp.status,
                'size' , jsonb_build_object(
                    'source', rp.source_shard_size,
                    'target', rp.target_shard_size),
                'hosts', jsonb_build_object(
                    'source', rp.sourcename || ':' || rp.sourceport,
                    'target', rp.targetname || ':' || rp.targetport),
                'message', t.message,
                'command', t.command,
                'task_id', t.task_id ) ||
            CASE
                WHEN ($2) THEN jsonb_build_object(
                    'size', jsonb_build_object(
                        'source', rp.source_shard_size,
                        'target', rp.target_shard_size),
                    'LSN', jsonb_build_object(
                        'source', rp.source_lsn,
                        'target', rp.target_lsn,
                        'lag', rp.source_lsn - rp.target_lsn))
                ELSE jsonb_build_object(
                    'size', jsonb_build_object(
                        'source', pg_size_pretty(rp.source_shard_size),
                        'target', pg_size_pretty(rp.target_shard_size)),
                    'LSN', jsonb_build_object(
                        'source', rp.source_lsn,
                        'target', rp.target_lsn,
                        'lag', pg_size_pretty(rp.source_lsn - rp.target_lsn)))
            END) AS tasks
        FROM
            rp JOIN pg_dist_background_task t ON rp.sessionid = t.pid
            JOIN pg_dist_background_job j ON t.job_id = j.job_id
        WHERE j.job_id = $1
            AND t.status = 'running'
    ),
    errored_or_retried_task_details AS (
        SELECT jsonb_agg(jsonb_build_object(
                'state', t.status,
                'retried', coalesce(t.retry_count,0),
                'message', t.message,
                'command', t.command,
                'task_id', t.task_id )) AS tasks
        FROM
            pg_dist_background_task t JOIN pg_dist_background_job j ON t.job_id = j.job_id
        WHERE j.job_id = $1
            AND NOT EXISTS (SELECT 1 FROM rp WHERE rp.sessionid = t.pid)
            AND (t.status = 'error' OR (t.status = 'runnable' AND t.retry_count > 0))
    )
    SELECT
        job_id,
        state,
        job_type,
        description,
        started_at,
        finished_at,
        jsonb_build_object(
            'task_state_counts', (SELECT jsonb_object_agg(status, count) FROM task_state_occurence_counts),
            'tasks', (COALESCE((SELECT tasks FROM running_task_details),'[]'::jsonb) ||
                      COALESCE((SELECT tasks FROM errored_or_retried_task_details),'[]'::jsonb))) AS details
    FROM pg_dist_background_job j
    WHERE j.job_id = $1
$fn$;
