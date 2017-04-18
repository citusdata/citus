--
-- WORKER_RANGE_PARTITION
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1160000;


\set JobId 201010
\set TaskId 101101
\set Partition_Column l_orderkey
\set Partition_Column_Text '\'l_orderkey\''
\set Partition_Column_Type 20

\set Select_Query_Text '\'SELECT * FROM lineitem\''
\set Select_All 'SELECT *'

\set Table_Part_00 lineitem_range_part_00
\set Table_Part_01 lineitem_range_part_01
\set Table_Part_02 lineitem_range_part_02
\set Table_Part_03 lineitem_range_part_03

\set File_Basedir  base/pgsql_job_cache
\set Table_File_00 :File_Basedir/job_:JobId/task_:TaskId/p_00000
\set Table_File_01 :File_Basedir/job_:JobId/task_:TaskId/p_00001
\set Table_File_02 :File_Basedir/job_:JobId/task_:TaskId/p_00002
\set Table_File_03 :File_Basedir/job_:JobId/task_:TaskId/p_00003

-- Run select query, and apply range partitioning on query results

SELECT worker_range_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Partition_Column_Text, :Partition_Column_Type,
				    ARRAY[1, 3000, 12000]::_int8);

COPY :Table_Part_00 FROM :'Table_File_00';
COPY :Table_Part_01 FROM :'Table_File_01';
COPY :Table_Part_02 FROM :'Table_File_02';
COPY :Table_Part_03 FROM :'Table_File_03';

SELECT COUNT(*) FROM :Table_Part_00;
SELECT COUNT(*) FROM :Table_Part_03;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_All FROM :Table_Part_00 EXCEPT ALL
       :Select_All FROM lineitem WHERE :Partition_Column < 1 ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_All FROM :Table_Part_01 EXCEPT ALL
       :Select_All FROM lineitem WHERE :Partition_Column >= 1 AND
       		   		       :Partition_Column < 3000 ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :Select_All FROM :Table_Part_02 EXCEPT ALL
       :Select_All FROM lineitem WHERE :Partition_Column >= 3000 AND
       		   		       :Partition_Column < 12000 ) diff;
SELECT COUNT(*) AS diff_lhs_03 FROM (
       :Select_All FROM :Table_Part_03 EXCEPT ALL
       :Select_All FROM lineitem WHERE :Partition_Column >= 12000 ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       :Select_All FROM lineitem WHERE :Partition_Column < 1 EXCEPT ALL
       :Select_All FROM :Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_All FROM lineitem WHERE :Partition_Column >= 1 AND
       		   		       :Partition_Column < 3000 EXCEPT ALL
       :Select_All FROM :Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM lineitem WHERE :Partition_Column >= 3000 AND
       		   		       :Partition_Column < 12000 EXCEPT ALL
       :Select_All FROM :Table_Part_02 ) diff;
SELECT COUNT(*) AS diff_rhs_03 FROM (
       :Select_All FROM lineitem WHERE :Partition_Column >= 12000 EXCEPT ALL
       :Select_All FROM :Table_Part_03 ) diff;
