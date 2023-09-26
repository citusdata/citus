SELECT nextval('pg_dist_shardid_seq') > MAX(shardid) FROM pg_dist_shard;
SELECT nextval('pg_dist_placement_placementid_seq') > MAX(placementid) FROM pg_dist_placement;
SELECT nextval('pg_dist_groupid_seq') > MAX(groupid) FROM pg_dist_node;
SELECT nextval('pg_dist_node_nodeid_seq') > MAX(nodeid) FROM pg_dist_node;
SELECT nextval('pg_dist_colocationid_seq') > MAX(colocationid) FROM pg_dist_colocation;

-- while testing sequences on pg_dist_cleanup, they return null in pg upgrade schedule
-- but return a valid value in citus upgrade schedule
-- that's why we accept both NULL and MAX()+1 here
SELECT
    CASE WHEN MAX(operation_id) IS NULL
    THEN true
    ELSE nextval('pg_dist_operationid_seq') > MAX(operation_id)
    END AS check_operationid
    FROM pg_dist_cleanup;
SELECT
    CASE WHEN MAX(record_id) IS NULL
    THEN true
    ELSE nextval('pg_dist_cleanup_recordid_seq') > MAX(record_id)
    END AS check_recordid
    FROM pg_dist_cleanup;
SELECT nextval('pg_dist_background_job_job_id_seq') > COALESCE(MAX(job_id), 0) FROM pg_dist_background_job;
SELECT nextval('pg_dist_background_task_task_id_seq') > COALESCE(MAX(task_id), 0) FROM pg_dist_background_task;
SELECT last_value > 0 FROM pg_dist_clock_logical_seq;

-- If this query gives output it means we've added a new sequence that should
-- possibly be restored after upgrades.
SELECT sequence_name FROM information_schema.sequences
  WHERE sequence_name LIKE 'pg_dist_%'
  AND sequence_name NOT IN (
    -- these ones are restored above
    'pg_dist_shardid_seq',
    'pg_dist_placement_placementid_seq',
    'pg_dist_groupid_seq',
    'pg_dist_node_nodeid_seq',
    'pg_dist_colocationid_seq',
    'pg_dist_operationid_seq',
    'pg_dist_cleanup_recordid_seq',
    'pg_dist_background_job_job_id_seq',
    'pg_dist_background_task_task_id_seq',
    'pg_dist_clock_logical_seq'
  );
