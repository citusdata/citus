--
-- WORKER_BINARY_DATA_PARTITION
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1090000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1090000;


\set JobId 201010
\set TaskId 101105
\set Partition_Column textcolumn
\set Partition_Column_Text '\'textcolumn\''
\set Partition_Column_Type 25

\set Select_Query_Text '\'SELECT * FROM binary_data_table\''
\set Select_All 'SELECT *'

\set Table_Name binary_data_table
\set Table_Part_00 binary_data_table_part_00
\set Table_Part_01 binary_data_table_part_01
\set Table_Part_02 binary_data_table_part_02

-- Create table with special characters

CREATE TABLE :Table_Name(textcolumn text, binarycolumn bytea);
COPY :Table_Name FROM stdin;
aaa	\013\120
binary data first	\012\120\20\21
binary data second	\21\120\130
binary data hex	\x1E\x0D
binary data with tabs	\012\t\120\v
some\t tabs\t with \t spaces	text with tabs
some\\ special\n characters \b	text with special characters
some ' and " and '' characters	text with quotes
\N	null text
\N	null text 2
\N	null text 3
\\N	actual backslash N value
\NN	null string and N
	empty string
\.

SELECT length(binarycolumn) FROM :Table_Name;

-- Run select query, and apply range partitioning on query results

SELECT worker_range_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Partition_Column_Text, :Partition_Column_Type,
				    ARRAY['aaa', 'some']::_text);

-- Copy range partitioned files into tables

CREATE TABLE :Table_Part_00 ( LIKE :Table_Name );
CREATE TABLE :Table_Part_01 ( LIKE :Table_Name );
CREATE TABLE :Table_Part_02 ( LIKE :Table_Name );

COPY :Table_Part_00 FROM 'base/pgsql_job_cache/job_201010/task_101105/p_00000';
COPY :Table_Part_01 FROM 'base/pgsql_job_cache/job_201010/task_101105/p_00001';
COPY :Table_Part_02 FROM 'base/pgsql_job_cache/job_201010/task_101105/p_00002';

-- The union of the three partitions should have as many rows as original table

SELECT COUNT(*) AS total_row_count FROM (
       SELECT * FROM :Table_Part_00 UNION ALL
       SELECT * FROM :Table_Part_01 UNION ALL
       SELECT * FROM :Table_Part_02 ) AS all_rows;

-- We first compute the difference of partition tables against the base table.
-- Then, we compute the difference of the base table against partitioned tables.

SELECT COUNT(*) AS diff_lhs_00 FROM (
       :Select_All FROM :Table_Part_00 EXCEPT ALL
       :Select_All FROM :Table_Name WHERE :Partition_Column IS NULL OR
       		   		    	  :Partition_Column < 'aaa' ) diff;
SELECT COUNT(*) AS diff_lhs_01 FROM (
       :Select_All FROM :Table_Part_01 EXCEPT ALL
       :Select_All FROM :Table_Name WHERE :Partition_Column >= 'aaa' AND
       		   		       	  :Partition_Column < 'some' ) diff;
SELECT COUNT(*) AS diff_lhs_02 FROM (
       :Select_All FROM :Table_Part_02 EXCEPT ALL
       :Select_All FROM :Table_Name WHERE :Partition_Column >= 'some' ) diff;

SELECT COUNT(*) AS diff_rhs_00 FROM (
       :Select_All FROM :Table_Name WHERE :Partition_Column IS NULL OR
       		   		    	  :Partition_Column < 'aaa' EXCEPT ALL
       :Select_All FROM :Table_Part_00 ) diff;
SELECT COUNT(*) AS diff_rhs_01 FROM (
       :Select_All FROM :Table_Name WHERE :Partition_Column >= 'aaa' AND
       		   		       	  :Partition_Column < 'some' EXCEPT ALL
       :Select_All FROM :Table_Part_01 ) diff;
SELECT COUNT(*) AS diff_rhs_02 FROM (
       :Select_All FROM :Table_Name WHERE :Partition_Column >= 'some' EXCEPT ALL
       :Select_All FROM :Table_Part_02 ) diff;
