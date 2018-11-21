--
-- WORKER_HASH_PARTITION
--



\set JobId 201010
\set TaskId 101103
\set Partition_Column l_orderkey
\set Partition_Column_Text '\'l_orderkey\''
\set Partition_Column_Type '\'int8\''
\set Partition_Count 4
\set hashTokenIncrement 1073741824
\set Select_Query_Text '\'SELECT * FROM lineitem\''
\set Select_All 'SELECT *'

-- Hash functions is mapped to exactly behave as Citus planner does
\set Hash_Mod_Function '( hashint8(l_orderkey)::int8 - (-2147483648))::int8 / :hashTokenIncrement::int8'

\set Table_Part_00 lineitem_hash_part_00
\set Table_Part_01 lineitem_hash_part_01
\set Table_Part_02 lineitem_hash_part_02
\set Table_Part_03 lineitem_hash_part_03

SELECT usesysid AS userid FROM pg_user WHERE usename = current_user \gset

\set File_Basedir  base/pgsql_job_cache
\set Table_File_00 :File_Basedir/job_:JobId/task_:TaskId/p_00000.:userid
\set Table_File_01 :File_Basedir/job_:JobId/task_:TaskId/p_00001.:userid
\set Table_File_02 :File_Basedir/job_:JobId/task_:TaskId/p_00002.:userid
\set Table_File_03 :File_Basedir/job_:JobId/task_:TaskId/p_00003.:userid

-- Run select query, and apply hash partitioning on query results

SELECT worker_hash_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				   :Partition_Column_Text, :Partition_Column_Type::regtype,
				   ARRAY[-2147483648, -1073741824, 0, 1073741824]::int4[]);

COPY :Table_Part_00 FROM :'Table_File_00';
COPY :Table_Part_01 FROM :'Table_File_01';
COPY :Table_Part_02 FROM :'Table_File_02';
COPY :Table_Part_03 FROM :'Table_File_03';

SELECT COUNT(*) FROM :Table_Part_00;
SELECT COUNT(*) FROM :Table_Part_01;
SELECT COUNT(*) FROM :Table_Part_02;
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
