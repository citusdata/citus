--
-- WORKER_HASH_PARTITION_COMPLEX
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1120000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1120000;


\set JobId 201010
\set TaskId 101104
\set Partition_Column l_partkey
\set Partition_Column_Text '\'l_partkey\''
\set Partition_Column_Type 23
\set Partition_Count 4

\set Select_Columns 'SELECT l_partkey, l_discount, l_shipdate, l_comment'
\set Select_Filters 'l_shipdate >= date \'1992-01-15\' AND l_discount between 0.02 AND 0.08'

\set Hash_Mod_Function '( (hashint4(l_partkey) & 2147483647) % 4 )'

\set Table_Part_00 lineitem_hash_complex_part_00
\set Table_Part_01 lineitem_hash_complex_part_01
\set Table_Part_02 lineitem_hash_complex_part_02
\set Table_Part_03 lineitem_hash_complex_part_03

-- Run hardcoded complex select query, and apply hash partitioning on query
-- results

SELECT worker_hash_partition_table(:JobId, :TaskId,
				   'SELECT l_partkey, l_discount, l_shipdate, l_comment'
				   ' FROM lineitem '
				   ' WHERE l_shipdate >= date ''1992-01-15'''
				   ' AND l_discount between 0.02 AND 0.08',
				   :Partition_Column_Text, :Partition_Column_Type,
				   :Partition_Count);

-- Copy partitioned data files into tables for testing purposes

COPY :Table_Part_00 FROM 'base/pgsql_job_cache/job_201010/task_101104/p_00000';
COPY :Table_Part_01 FROM 'base/pgsql_job_cache/job_201010/task_101104/p_00001';
COPY :Table_Part_02 FROM 'base/pgsql_job_cache/job_201010/task_101104/p_00002';
COPY :Table_Part_03 FROM 'base/pgsql_job_cache/job_201010/task_101104/p_00003';

SELECT COUNT(*) FROM :Table_Part_00;
SELECT COUNT(*) FROM :Table_Part_03;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_Columns FROM :Table_Part_00 EXCEPT ALL
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 0) ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_Columns FROM :Table_Part_01 EXCEPT ALL
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 1) ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :Select_Columns FROM :Table_Part_02 EXCEPT ALL
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 2) ) diff;
SELECT COUNT(*) AS diff_lhs_03 FROM (
       :Select_Columns FROM :Table_Part_03 EXCEPT ALL
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 3) ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 0) EXCEPT ALL
       :Select_Columns FROM :Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 1) EXCEPT ALL
       :Select_Columns FROM :Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 2) EXCEPT ALL
       :Select_Columns FROM :Table_Part_02 ) diff;
SELECT COUNT(*) AS diff_rhs_03 FROM (
       :Select_Columns FROM lineitem WHERE :Select_Filters AND
       		       	    	     	   (:Hash_Mod_Function = 3) EXCEPT ALL
       :Select_Columns FROM :Table_Part_03 ) diff;
