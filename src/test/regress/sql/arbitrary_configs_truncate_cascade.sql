SET search_path TO truncate_cascade_tests_schema;

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
