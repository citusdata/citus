CREATE SCHEMA citus_aggregated_stats;
SET search_path TO citus_aggregated_stats, public;
SET citus.shard_count = 2;
SET citus.next_shard_id TO 1870000;

CREATE USER user1;
GRANT ALL ON SCHEMA citus_aggregated_stats TO user1;

SET SESSION AUTHORIZATION user1;

CREATE TABLE current_check (currentid int, payload text, rlsuser text);
GRANT ALL ON current_check TO PUBLIC;

INSERT INTO current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

ALTER TABLE current_check ENABLE ROW LEVEL SECURITY;

SET row_security TO ON;

ANALYZE current_check;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

-- test various Citus table types in citus_stats
CREATE TABLE dist_current_check (currentid int, payload text, rlsuser text);
CREATE TABLE ref_current_check (currentid int, payload text, rlsuser text);
CREATE TABLE citus_local_current_check (currentid int, payload text, rlsuser text);

SELECT create_distributed_table('dist_current_check', 'currentid');
SELECT create_reference_table('ref_current_check');
SELECT citus_add_local_table_to_metadata('citus_local_current_check');

INSERT INTO dist_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

INSERT INTO ref_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

INSERT INTO citus_local_current_check VALUES
    (1, 'abc', 'user1'),
    (3, 'cde', 'user1'),
    (4, 'def', 'user1'),
    (4, 'def', 'user1'),
    (3, 'cde', 'user2'),
    (5, NULL, NULL),
    (4, 'def', 'user1');

ANALYZE dist_current_check;
ANALYZE ref_current_check;
ANALYZE citus_local_current_check;

SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

SELECT * FROM citus_stats
  WHERE tablename IN ('current_check', 'dist_current_check', 'ref_current_check', 'citus_local_current_check')
  ORDER BY 1;

-- create a dist table with million rows to simulate 3.729223e+06 in reltuples
-- this tests casting numbers like 3.729223e+06 to bigint

CREATE TABLE organizations (
    org_id bigint,
    id int
);

SELECT create_distributed_table('organizations', 'org_id');

INSERT INTO organizations(org_id, id)
  SELECT i, 1
  FROM generate_series(1,2000000) i;

ANALYZE organizations;

SELECT attname, null_frac, most_common_vals, most_common_freqs FROM citus_stats
  WHERE tablename IN ('organizations')
  ORDER BY 1;

-- more real-world scenario:
-- outputs of pg_stats and citus_stats are NOT the same
-- but citus_stats does a fair estimation job

SELECT setseed(0.42);

CREATE TABLE orders (id bigint , custid int, product text, quantity int);

INSERT INTO orders(id, custid, product, quantity)
SELECT i, (random() * 100)::int, 'product' || (random() * 10)::int, NULL
FROM generate_series(1,11) d(i);

-- frequent customer
INSERT INTO orders(id, custid, product, quantity)
SELECT 1200, 17, 'product' || (random() * 10)::int, NULL
FROM generate_series(1, 57) sk(i);

-- popular product
INSERT INTO orders(id, custid, product, quantity)
SELECT i+100 % 17, NULL, 'product3', (random() * 40)::int
FROM generate_series(1, 37) sk(i);

-- frequent customer
INSERT INTO orders(id, custid, product, quantity)
SELECT 1390, 76, 'product' || ((random() * 20)::int % 3), (random() * 30)::int
FROM generate_series(1, 33) sk(i);

ANALYZE orders;

-- pg_stats
SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM pg_stats
  WHERE tablename IN ('orders')
ORDER BY 3;

SELECT create_distributed_table('orders', 'id');
ANALYZE orders;

-- citus_stats
SELECT schemaname, tablename, attname, null_frac, most_common_vals, most_common_freqs FROM citus_stats
  WHERE tablename IN ('orders')
ORDER BY 3;

RESET SESSION AUTHORIZATION;
DROP SCHEMA citus_aggregated_stats CASCADE;
DROP USER user1;
