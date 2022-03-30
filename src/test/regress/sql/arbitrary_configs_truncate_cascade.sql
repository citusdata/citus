SET search_path TO truncate_cascade_tests_schema;

-- Test truncate error on table with dependencies
TRUNCATE table_with_pk;

-- Test truncate rollback on table with dependencies
SELECT COUNT(*) FROM table_with_fk_1;
SELECT COUNT(*) FROM table_with_fk_2;

BEGIN;
TRUNCATE table_with_pk CASCADE;
SELECT COUNT(*) FROM table_with_fk_1;
SELECT COUNT(*) FROM table_with_fk_2;
ROLLBACK;

SELECT COUNT(*) FROM table_with_fk_1;
SELECT COUNT(*) FROM table_with_fk_2;

-- Test truncate on table with dependencies
SELECT COUNT(*) FROM table_with_pk;
SELECT COUNT(*) FROM table_with_fk_1;
SELECT COUNT(*) FROM table_with_fk_2;

TRUNCATE table_with_pk CASCADE;

SELECT COUNT(*) FROM table_with_pk;
SELECT COUNT(*) FROM table_with_fk_1;
SELECT COUNT(*) FROM table_with_fk_2;
