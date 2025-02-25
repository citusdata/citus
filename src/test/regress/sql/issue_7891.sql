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

CREATE TABLE t2_ref2 (
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

-- Mark t2_ref and t2_ref2 as a reference table
SELECT create_reference_table('t2_ref');
SELECT create_reference_table('t2_ref2');

-- Insert sample data
INSERT INTO t6_pg (vkey, pkey, c26) VALUES
    (2, 12000, 'initial'),
    (3, 13000, 'will_be_deleted'),
    (4, 14000, 'to_merge');
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

SELECT 't6_pg after' AS label, * FROM t6_pg;

--
--    DELETE with a similar nested subquery approach
--    Here, let's delete any rows for which t4_pg is non-empty (like a trivial check).
--    We'll specifically target the row with c26='will_be_deleted' to confirm it's removed.
--
DELETE FROM t6_pg
 WHERE EXISTS (
   SELECT (SELECT c15 FROM t2_ref)
   FROM t4_pg
 )
 AND c26 = 'will_be_deleted';

SELECT 't6_pg after DELETE' AS label, * FROM t6_pg;

--
--    We'll merge from t4_pg into t6_pg. The merge will update c26 for pkey=14000.
--
-- Anticipate an error indicating non-IMMUTABLE functions are not supported in MERGE statements on distributed tables.
-- Retain this comment to highlight the current limitation.
--
MERGE INTO t6_pg AS tgt
USING t4_pg AS src
ON (tgt.pkey = 14000)
WHEN MATCHED THEN
    UPDATE SET c26 = 'merged_' || (SELECT pkey FROM t2_ref WHERE pkey=24000 LIMIT 1)
WHEN NOT MATCHED THEN
    INSERT (vkey, pkey, c26)
    VALUES (99, src.pkey, 'inserted_via_merge');

MERGE INTO t2_ref AS tgt
USING t4_pg AS src
  ON (tgt.pkey = src.pkey)
WHEN MATCHED THEN
  UPDATE SET c15 = '2088-01-01 00:00:00'::timestamp
WHEN NOT MATCHED THEN
  INSERT (vkey, pkey, c15)
  VALUES (src.vkey, src.pkey, '2099-12-31 23:59:59'::timestamp);

-- Show the final state of t2_ref:
SELECT 't2_ref after MERGE (using t4_pg)' AS label, * FROM t2_ref;

MERGE INTO t2_ref2 AS tgt
USING t2_ref AS src
  ON (tgt.pkey = src.pkey)
WHEN MATCHED THEN
  UPDATE SET c15 = '2077-07-07 07:07:07'::timestamp
WHEN NOT MATCHED THEN
  INSERT (vkey, pkey, c15)
  VALUES (src.vkey, src.pkey, '2066-06-06 06:06:06'::timestamp);

-- Show the final state of t2_ref2:
SELECT 't2_ref2 after MERGE (using t2_ref)' AS label, * FROM t2_ref2;


MERGE INTO t6_pg AS tgt
USING t4_pg AS src
  ON (tgt.pkey = src.pkey)
WHEN MATCHED THEN
  UPDATE SET c26 = 'merged_value'
WHEN NOT MATCHED THEN
  INSERT (vkey, pkey, c26)
  VALUES (src.vkey, src.pkey, 'inserted_via_merge');

SELECT 't6_pg after MERGE' AS label, * FROM t6_pg;

--
--   Update the REFERENCE table itself and verify the change
--   This is to ensure that the reference table is correctly handled.

UPDATE t2_ref
   SET c15 = '2099-01-01 00:00:00'::timestamp
 WHERE pkey = 24000;

SELECT 't2_ref after self-update' AS label, * FROM t2_ref;


UPDATE t2_ref
   SET c15 = '2099-01-01 00:00:00'::timestamp
 WHERE EXISTS (
    SELECT 1
    FROM t4_pg
 );

SELECT 't2_ref after UPDATE' AS label, * FROM t2_ref;

-- Cleanup
SET client_min_messages TO WARNING;
DROP SCHEMA issue_7891 CASCADE;
