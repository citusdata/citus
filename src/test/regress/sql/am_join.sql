CREATE SCHEMA am_cstore_join;
SET search_path TO am_cstore_join;

CREATE TABLE users (id int, name text) USING columnar;
INSERT INTO users SELECT a, 'name' || a FROM generate_series(0,30-1) AS a;

CREATE TABLE things (id int, user_id int, name text) USING columnar;
INSERT INTO things SELECT a, a % 30, 'thing' || a FROM generate_series(1,300) AS a;

-- force the nested loop to rescan the table
SET enable_material TO off;
SET enable_hashjoin TO off;
SET enable_mergejoin TO off;

SELECT count(*)
FROM users
JOIN things ON (users.id = things.user_id)
WHERE things.id > 290;

-- verify the join uses a nested loop to trigger the rescan behaviour
EXPLAIN (COSTS OFF)
SELECT count(*)
FROM users
JOIN things ON (users.id = things.user_id)
WHERE things.id > 299990;

SET client_min_messages TO warning;
DROP SCHEMA am_cstore_join CASCADE;
