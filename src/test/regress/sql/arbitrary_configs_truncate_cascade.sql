SET search_path TO truncate_cascade_tests_schema;

-- Hide detail of truncate error because it might either reference
-- table_with_fk_1 or table_with_fk_2 in the error message.
\set VERBOSITY TERSE

-- Test truncate error on table with dependencies
TRUNCATE table_with_pk;

\set VERBOSITY DEFAULT

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
