-- This test validates that the query planner correctly handles nested subqueries involving both a
-- local table (t4_pg) and a reference table (t2_ref). The steps are as follows:
--
-- 1. A dedicated schema (issue_7891) is created, and three tables (t2_ref, t4_pg, t6_pg) are set up.
-- 2. The table t2_ref is designated as a reference table using the create_reference_table() function.
-- 3. Sample data is inserted into all tables.
-- 4. An UPDATE is executed on t6_pg. The update uses an EXISTS clause with a nested subquery:
--    - The outer subquery iterates over every row in t4_pg.
--    - The inner subquery selects c15 from t2_ref.
-- 5. The update should occur if the nested subquery returns any row, effectively updating t6_pg's vkey to 43.
-- 6. The final state of t6_pg is displayed to confirm that the update was applied.
--
-- Note: This test was originally designed to detect a planner bug where the nested structure might
-- lead to an incorrect plan (such as a 0-task plan), ensuring proper handling of reference and local tables.
-- https://github.com/citusdata/citus/issues/7891
CREATE SCHEMA issue_7891;
SET search_path TO issue_7891;

-- Create tables
CREATE TABLE t2_ref (
    vkey INT,
    pkey INT,
    c15 TIMESTAMP
);

CREATE TABLE t4_pg (
    vkey INT,
    pkey INT,
    c22 NUMERIC,
    c23 TEXT,
    c24 TIMESTAMP
);

CREATE TABLE t6_pg (
    vkey INT,
    pkey INT,
    c26 TEXT
);

-- Mark t2_ref as a reference table
SELECT create_reference_table('t2_ref');

-- Insert sample data
INSERT INTO t6_pg (vkey, pkey, c26) VALUES (2, 12000, '');
INSERT INTO t4_pg (vkey, pkey, c22, c23, c24)
    VALUES (5, 15000, 0.0, ']]?', MAKE_TIMESTAMP(2071, 10, 26, 16, 20, 5));
INSERT INTO t2_ref (vkey, pkey, c15)
    VALUES (14, 24000, NULL::timestamp);

-- Show initial data
SELECT 't6_pg before' AS label, * FROM t6_pg;
SELECT 't4_pg data' AS label, * FROM t4_pg;
SELECT 't2_ref data' AS label, * FROM t2_ref;

--
-- The problematic query: update t6_pg referencing t4_pg and sub-subquery on t2_ref.
-- Historically might produce a 0-task plan if the planner incorrectly fails to
-- treat t4_pg/t2_ref as local/reference.
--

-- The outer subquery iterates over every row in table t4_pg.
UPDATE t6_pg
   SET vkey = 43
 WHERE EXISTS (
   SELECT (SELECT c15 FROM t2_ref)
   FROM t4_pg
);

-- Show final data
SELECT 't6_pg after' AS label, * FROM t6_pg;

-- Cleanup
SET client_min_messages TO WARNING;
DROP SCHEMA issue_7891 CASCADE;
