SET search_path TO truncate_partition_tests_schema;

-- Test truncate on a partition
SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_0;
SELECT COUNT(*) FROM partitioned_table_1;

TRUNCATE partitioned_table_0;

SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_0;
SELECT COUNT(*) FROM partitioned_table_1;
