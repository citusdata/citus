--
-- WORKER_MERGE_RANGE_FILES
--


ALTER SEQUENCE pg_catalog.pg_dist_shardid_seq RESTART 1150000;
ALTER SEQUENCE pg_catalog.pg_dist_jobid_seq RESTART 1150000;


\set JobId 201010
\set TaskId 101101
\set Task_Table_Name public.task_101101
\set Select_All 'SELECT *'

-- TaskId determines our dependency on range partitioned files. We take these
-- files, and merge them in a task table. We also pass the column names and
-- column types that are used to create the task table.

SELECT worker_merge_files_into_table(:JobId, :TaskId,
       ARRAY['orderkey', 'partkey', 'suppkey', 'linenumber', 'quantity', 'extendedprice',
             'discount', 'tax', 'returnflag', 'linestatus', 'shipdate', 'commitdate',
	     'receiptdate', 'shipinstruct', 'shipmode', 'comment']::_text,
       ARRAY['bigint', 'integer', 'integer', 'integer', 'decimal(15, 2)', 'decimal(15, 2)',
             'decimal(15, 2)', 'decimal(15, 2)', 'char(1)', 'char(1)', 'date', 'date',
	     'date', 'char(25)', 'char(10)', 'varchar(44)']::_text);


-- We first count elements from the merged table and the original table we range
-- partitioned. We then compute the difference of these two tables.

SELECT COUNT(*) FROM :Task_Table_Name;
SELECT COUNT(*) FROM lineitem;

SELECT COUNT(*) AS diff_lhs FROM ( :Select_All FROM :Task_Table_Name EXCEPT ALL
       		   	    	   :Select_All FROM lineitem ) diff;

SELECT COUNT(*) AS diff_rhs FROM ( :Select_All FROM lineitem EXCEPT ALL
       		   	    	   :Select_All FROM :Task_Table_Name ) diff;
