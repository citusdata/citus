--
-- TASK_TRACKER_PARTITION_TASK
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1080000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1080000;


\set JobId 401010
\set PartitionTaskId 801106

\set PartitionColumn l_orderkey
\set SelectAll 'SELECT *'

\set TablePart00 lineitem_partition_task_part_00
\set TablePart01 lineitem_partition_task_part_01
\set TablePart02 lineitem_partition_task_part_02

-- We assign a partition task and wait for it to complete. Note that we hardcode
-- the partition function call string, including the job and task identifiers,
-- into the argument in the task assignment function. This hardcoding is
-- necessary as the current psql version does not perform variable interpolation
-- for names inside single quotes.

SELECT task_tracker_assign_task(:JobId, :PartitionTaskId,
       				'SELECT worker_range_partition_table('
				'401010, 801106, ''SELECT * FROM lineitem'', '
				'''l_orderkey'', 20, ARRAY[1000, 3000]::_int8)');

SELECT pg_sleep(4.0);

SELECT task_tracker_task_status(:JobId, :PartitionTaskId);

COPY :TablePart00 FROM 'base/pgsql_job_cache/job_401010/task_801106/p_00000';
COPY :TablePart01 FROM 'base/pgsql_job_cache/job_401010/task_801106/p_00001';
COPY :TablePart02 FROM 'base/pgsql_job_cache/job_401010/task_801106/p_00002';

SELECT COUNT(*) FROM :TablePart00;
SELECT COUNT(*) FROM :TablePart02;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :SelectAll FROM :TablePart00 EXCEPT ALL
       :SelectAll FROM lineitem WHERE :PartitionColumn < 1000 ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :SelectAll FROM :TablePart01 EXCEPT ALL
       :SelectAll FROM lineitem WHERE :PartitionColumn >= 1000 AND
       		   		      :PartitionColumn < 3000 ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :SelectAll FROM :TablePart02 EXCEPT ALL
       :SelectAll FROM lineitem WHERE :PartitionColumn >= 3000 ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       :SelectAll FROM lineitem WHERE :PartitionColumn < 1000 EXCEPT ALL
       :SelectAll FROM :TablePart00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :SelectAll FROM lineitem WHERE :PartitionColumn >= 1000 AND
       		   		      :PartitionColumn < 3000 EXCEPT ALL
       :SelectAll FROM :TablePart01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :SelectAll FROM lineitem WHERE :PartitionColumn >= 3000 EXCEPT ALL
       :SelectAll FROM :TablePart02 ) diff;
