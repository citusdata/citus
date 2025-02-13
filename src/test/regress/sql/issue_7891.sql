-- This construct has been used as a regression test to ensure that the planner
-- correctly distinguishes between "local" and "reference" tables, avoiding an erroneous 0-task plan.
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

-- The outer subquery iterates over rows from the reference table t2_ref
UPDATE t6_pg
   SET vkey = 44
 WHERE EXISTS (
   SELECT (SELECT c22 FROM t4_pg)
   FROM t2_ref
);

-- Show final data
SELECT 't6_pg after' AS label, * FROM t6_pg;

SET client_min_messages TO WARNING;
DROP SCHEMA issue_7891 CASCADE;
