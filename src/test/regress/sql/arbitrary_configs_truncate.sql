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

-- Test truncate on a partition
SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_0;
SELECT COUNT(*) FROM partitioned_table_1;

TRUNCATE partitioned_table_0;

SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_0;
SELECT COUNT(*) FROM partitioned_table_1;

-- Test truncate a partioned
SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_1;

TRUNCATE partitioned_table;

SELECT COUNT(*) FROM partitioned_table;
SELECT COUNT(*) FROM partitioned_table_1;

-- Test truncate error on table with dependencies
TRUNCATE table_with_pk;

-- Test truncate rollback on table with dependencies
SELECT COUNT(*) FROM table_with_fk;

BEGIN;
TRUNCATE table_with_pk CASCADE;
SELECT COUNT(*) FROM table_with_fk;
ROLLBACK;

SELECT COUNT(*) FROM table_with_fk;

-- Test truncate on table with dependencies
SELECT COUNT(*) FROM table_with_pk;
SELECT COUNT(*) FROM table_with_fk;

TRUNCATE table_with_pk CASCADE;

SELECT COUNT(*) FROM table_with_pk;
SELECT COUNT(*) FROM table_with_fk;
