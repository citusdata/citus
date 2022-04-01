SET search_path TO truncate_tests_schema;

-- Test truncate rollback on a basic table
SELECT COUNT(*) FROM basic_table;

BEGIN;
TRUNCATE basic_table;
SELECT COUNT(*) FROM basic_table;
ROLLBACK;

SELECT COUNT(*) FROM basic_table;

-- Test truncate on a basic table
SELECT COUNT(*) FROM basic_table;

TRUNCATE basic_table;

SELECT COUNT(*) FROM basic_table;

-- Test trucate rollback on partitioned table
SELECT COUNT(*) FROM partitioned_table_0;

BEGIN;
TRUNCATE partitioned_table;
SELECT COUNT(*) FROM partitioned_table_0;
ROLLBACK;

SELECT COUNT(*) FROM partitioned_table_0;

-- Test truncate a partioned table
SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_1;

TRUNCATE partitioned_table;

SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_1;
