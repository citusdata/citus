--
-- WORKER_CHECK_INVALID_ARGUMENTS
--


SET citus.next_shard_id TO 1100000;


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
				    ARRAY[-2147483648, -1073741824, 0, 1073741824]::int4[]);

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

-- Check that we fail to merge when ids are wrong

SELECT worker_merge_files_into_table(-1, :TaskId,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'bytea']);

SELECT worker_merge_files_into_table(:JobId, -1,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'bytea']);

SELECT worker_merge_files_and_run_query(-1, :TaskId,
    'CREATE TABLE task_000001_merge(merge_column_0 int)',
    'CREATE TABLE task_000001 (a) AS SELECT sum(merge_column_0) FROM task_000001_merge'
);

SELECT worker_merge_files_and_run_query(:JobId, -1,
    'CREATE TABLE task_000001_merge(merge_column_0 int)',
    'CREATE TABLE task_000001 (a) AS SELECT sum(merge_column_0) FROM task_000001_merge'
);

-- Finally, merge partitioned files using valid arguments

SELECT worker_merge_files_into_table(:JobId, :TaskId,
       				     ARRAY['textcolumn', 'binarycolumn'],
				     ARRAY['text', 'bytea']);

-- worker_execute_sql_task should only accept queries
select worker_execute_sql_task(0,0,'create table foo(a serial)',false);
