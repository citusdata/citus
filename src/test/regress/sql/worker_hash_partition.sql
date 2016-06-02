--
-- WORKER_HASH_PARTITION
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1130000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1130000;


\set JobId 201010
\set TaskId 101103
\set Partition_Column l_orderkey
\set Partition_Column_Text '\'l_orderkey\''
\set Partition_Column_Type 20
\set Partition_Count 4

\set Select_Query_Text '\'SELECT * FROM lineitem\''
\set Select_All 'SELECT *'

-- Hash functions internally return unsigned 32-bit integers. However, when
-- called externally, the return value becomes a signed 32-bit integer. We hack
-- around this conversion issue by bitwise-anding the hash results. Note that
-- this only works because we are modding with 4. The proper Hash_Mod_Function
-- would be (case when hashint8(l_orderkey) >= 0 then (hashint8(l_orderkey) % 4)
-- else ((hashint8(l_orderkey) + 4294967296) % 4) end).

\set Hash_Mod_Function '( (hashint8(l_orderkey) & 2147483647) % 4 )'

\set Table_Part_00 lineitem_hash_part_00
\set Table_Part_01 lineitem_hash_part_01
\set Table_Part_02 lineitem_hash_part_02
\set Table_Part_03 lineitem_hash_part_03

-- Run select query, and apply hash partitioning on query results

SELECT worker_hash_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				   :Partition_Column_Text, :Partition_Column_Type,
				   :Partition_Count);

COPY :Table_Part_00 FROM 'base/pgsql_job_cache/job_201010/task_101103/p_00000';
COPY :Table_Part_01 FROM 'base/pgsql_job_cache/job_201010/task_101103/p_00001';
COPY :Table_Part_02 FROM 'base/pgsql_job_cache/job_201010/task_101103/p_00002';
COPY :Table_Part_03 FROM 'base/pgsql_job_cache/job_201010/task_101103/p_00003';

SELECT COUNT(*) FROM :Table_Part_00;
SELECT COUNT(*) FROM :Table_Part_03;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_All FROM :Table_Part_00 EXCEPT ALL
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 0) ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_All FROM :Table_Part_01 EXCEPT ALL
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 1) ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :Select_All FROM :Table_Part_02 EXCEPT ALL
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 2) ) diff;
SELECT COUNT(*) AS diff_lhs_03 FROM (
       :Select_All FROM :Table_Part_03 EXCEPT ALL
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 3) ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 0) EXCEPT ALL
       :Select_All FROM :Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 1) EXCEPT ALL
       :Select_All FROM :Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 2) EXCEPT ALL
       :Select_All FROM :Table_Part_02 ) diff;
SELECT COUNT(*) AS diff_rhs_03 FROM (
       :Select_All FROM lineitem WHERE (:Hash_Mod_Function = 3) EXCEPT ALL
       :Select_All FROM :Table_Part_03 ) diff;
