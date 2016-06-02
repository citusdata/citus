--
-- WORKER_CHECK_INVALID_ARGUMENTS
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1100000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1100000;


\set JobId 201010
\set TaskId 101108
\set Table_Name simple_binary_data_table
\set Partition_Column_Name '\'textcolumn\''
\set Partition_Column_Type 25
\set Partition_Count 2
\set Select_Query_Text '\'SELECT * FROM simple_binary_data_table\''

\set Bad_Partition_Column_Name '\'badcolumnname\''
\set Bad_Partition_Column_Type 20
\set Bad_Select_Query_Text '\'SELECT * FROM bad_table_name\''

-- Create simple table and insert a few rows into this table
-- N.B. - These rows will be partitioned to files on disk then read back in the
-- order the files are listed by a call to readdir; because this order is not
-- predictable, the second column of these rows always has the same value, to
-- avoid an error message differing based on file read order.

CREATE TABLE :Table_Name(textcolumn text, binarycolumn bytea);
COPY :Table_Name FROM stdin;
aaa	\013\120
some\t tabs\t with \t spaces	\013\120
\.

SELECT COUNT(*) FROM :Table_Name;

-- Check that we fail with bad SQL query

SELECT worker_range_partition_table(:JobId, :TaskId, :Bad_Select_Query_Text,
       				    :Partition_Column_Name, :Partition_Column_Type,
				    ARRAY['aaa', 'some']::_text);

-- Check that we fail with bad partition column name

SELECT worker_range_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Bad_Partition_Column_Name, :Partition_Column_Type,
				    ARRAY['aaa', 'some']::_text);

-- Check that we fail when partition column and split point types do not match

SELECT worker_range_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Partition_Column_Name, :Bad_Partition_Column_Type,
				    ARRAY['aaa', 'some']::_text);

-- Check that we fail with bad partition column type on hash partitioning

SELECT worker_hash_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Partition_Column_Name, :Bad_Partition_Column_Type,
				    :Partition_Count);

-- Now, partition table data using valid arguments

SELECT worker_range_partition_table(:JobId, :TaskId, :Select_Query_Text,
       				    :Partition_Column_Name, :Partition_Column_Type,
				    ARRAY['aaa', 'some']::_text);

-- Check that we fail to merge when the number of column names and column types
-- do not match

SELECT worker_merge_files_into_table(:JobId, :TaskId,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'bytea', 'integer']);

-- Check that we fail to merge when column types do not match underlying data

SELECT worker_merge_files_into_table(:JobId, :TaskId,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'integer']);

-- Finally, merge partitioned files using valid arguments

SELECT worker_merge_files_into_table(:JobId, :TaskId,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'bytea']);
