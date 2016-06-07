--
-- WORKER_NULL_DATA_PARTITION
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1180000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1180000;


\set JobId 201010
\set Range_TaskId 101106
\set Partition_Column s_nationkey
\set Partition_Column_Text '\'s_nationkey\''
\set Partition_Column_Type 23

\set Select_Query_Text '\'SELECT * FROM supplier\''
\set Select_All 'SELECT *'

\set Range_Table_Part_00 supplier_range_part_00
\set Range_Table_Part_01 supplier_range_part_01
\set Range_Table_Part_02 supplier_range_part_02

-- Run select query, and apply range partitioning on query results. Note that
-- one of the split point values is 0, We are checking here that the partition
-- function doesn't treat 0 as null, and that range repartitioning correctly
-- puts null nation key values into the 0th repartition bucket.

SELECT worker_range_partition_table(:JobId, :Range_TaskId, :Select_Query_Text,
       				    :Partition_Column_Text, :Partition_Column_Type,
				    ARRAY[0, 10]::_int4);

-- Copy partitioned data files into tables for testing purposes

COPY :Range_Table_Part_00 FROM 'base/pgsql_job_cache/job_201010/task_101106/p_00000';
COPY :Range_Table_Part_01 FROM 'base/pgsql_job_cache/job_201010/task_101106/p_00001';
COPY :Range_Table_Part_02 FROM 'base/pgsql_job_cache/job_201010/task_101106/p_00002';

SELECT COUNT(*) FROM :Range_Table_Part_00;
SELECT COUNT(*) FROM :Range_Table_Part_02;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_All FROM :Range_Table_Part_00 EXCEPT ALL
       (:Select_All FROM supplier WHERE :Partition_Column < 0 OR
                                       	:Partition_Column IS NULL) ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_All FROM :Range_Table_Part_01 EXCEPT ALL
       :Select_All FROM supplier WHERE :Partition_Column >= 0 AND
       		   		       :Partition_Column < 10 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM supplier WHERE :Partition_Column >= 10 EXCEPT ALL
       :Select_All FROM :Range_Table_Part_02 ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       (:Select_All FROM supplier WHERE :Partition_Column < 0 OR
                                        :Partition_Column IS NULL) EXCEPT ALL
       :Select_All FROM :Range_Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_All FROM supplier WHERE :Partition_Column >= 0 AND
       		   		       :Partition_Column < 10 EXCEPT ALL
       :Select_All FROM :Range_Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM supplier WHERE :Partition_Column >= 10 EXCEPT ALL
       :Select_All FROM :Range_Table_Part_02 ) diff;


-- Next, run select query and apply hash partitioning on query results. We are
-- checking here that hash repartitioning correctly puts null nation key values
-- into the 0th repartition bucket.

\set Hash_TaskId 101107
\set Partition_Count 4
\set Hash_Mod_Function '( (hashint4(s_nationkey) & 2147483647) % 4 )'

\set Hash_Table_Part_00 supplier_hash_part_00
\set Hash_Table_Part_01 supplier_hash_part_01
\set Hash_Table_Part_02 supplier_hash_part_02

-- Run select query, and apply hash partitioning on query results

SELECT worker_hash_partition_table(:JobId, :Hash_TaskId, :Select_Query_Text,
       				   :Partition_Column_Text, :Partition_Column_Type,
				   :Partition_Count);

COPY :Hash_Table_Part_00 FROM 'base/pgsql_job_cache/job_201010/task_101107/p_00000';
COPY :Hash_Table_Part_01 FROM 'base/pgsql_job_cache/job_201010/task_101107/p_00001';
COPY :Hash_Table_Part_02 FROM 'base/pgsql_job_cache/job_201010/task_101107/p_00002';

SELECT COUNT(*) FROM :Hash_Table_Part_00;
SELECT COUNT(*) FROM :Hash_Table_Part_02;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_All FROM :Hash_Table_Part_00 EXCEPT ALL
       (:Select_All FROM supplier WHERE (:Hash_Mod_Function = 0) OR
                                       	 :Partition_Column IS NULL) ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_All FROM :Hash_Table_Part_01 EXCEPT ALL
       :Select_All FROM supplier WHERE (:Hash_Mod_Function = 1) ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :Select_All FROM :Hash_Table_Part_02 EXCEPT ALL
       :Select_All FROM supplier WHERE (:Hash_Mod_Function = 2) ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       (:Select_All FROM supplier WHERE (:Hash_Mod_Function = 0) OR
                                       	 :Partition_Column IS NULL) EXCEPT ALL
       :Select_All FROM :Hash_Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_All FROM supplier WHERE (:Hash_Mod_Function = 1) EXCEPT ALL
       :Select_All FROM :Hash_Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM supplier WHERE (:Hash_Mod_Function = 2) EXCEPT ALL
       :Select_All FROM :Hash_Table_Part_02 ) diff;
