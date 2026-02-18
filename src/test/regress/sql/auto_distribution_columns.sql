--
-- AUTO_DISTRIBUTION_COLUMNS
--
-- Tests for the citus.distribution_columns GUC that auto-distributes
-- tables by a priority list of column names on CREATE TABLE / CREATE TABLE AS SELECT.
--

SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;
SET citus.next_shard_id TO 7800000;

-- add a worker so we can actually distribute
SELECT 1 FROM citus_add_node('localhost', :worker_1_port);

-- ===== Basic: single column in list =====

SET citus.distribution_columns TO 'tenant_id';

CREATE TABLE t_basic (id bigserial, tenant_id bigint, data text);

-- verify it was auto-distributed by tenant_id
SELECT distribution_column FROM citus_tables WHERE table_name = 't_basic'::regclass;

DROP TABLE t_basic;

-- ===== Priority list: first match wins =====

SET citus.distribution_columns TO 'tenant_id, customer_id, department';

-- Table has tenant_id → should distribute by tenant_id
CREATE TABLE t_prio1 (id int, tenant_id int, customer_id int, department text);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_prio1'::regclass;

-- Table only has customer_id → should distribute by customer_id
CREATE TABLE t_prio2 (id int, customer_id int, department text);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_prio2'::regclass;

-- Table only has department → should distribute by department
CREATE TABLE t_prio3 (id int, department text);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_prio3'::regclass;

-- Table has none of the columns → should NOT be distributed
CREATE TABLE t_prio_none (id int, other_col text);
SELECT count(*) FROM citus_tables WHERE table_name = 't_prio_none'::regclass;

DROP TABLE t_prio1, t_prio2, t_prio3, t_prio_none;

-- ===== CREATE TABLE AS SELECT =====

-- source table (disable GUC while creating the source table explicitly)
RESET citus.distribution_columns;
CREATE TABLE source_data (id int, tenant_id int, val text);
SELECT create_distributed_table('source_data', 'tenant_id');
INSERT INTO source_data VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 10, 'c');

SET citus.distribution_columns TO 'tenant_id';

CREATE TABLE t_ctas AS SELECT * FROM source_data;

-- should be distributed by tenant_id
SELECT distribution_column FROM citus_tables WHERE table_name = 't_ctas'::regclass;

-- data should be there
SELECT count(*) FROM t_ctas;

DROP TABLE t_ctas;

-- CTAS with priority list fallback
SET citus.distribution_columns TO 'nonexistent, tenant_id';
CREATE TABLE t_ctas_fallback AS SELECT * FROM source_data;
SELECT distribution_column FROM citus_tables WHERE table_name = 't_ctas_fallback'::regclass;
DROP TABLE t_ctas_fallback;

DROP TABLE source_data;

-- ===== Whitespace handling in list =====

SET citus.distribution_columns TO '  tenant_id  ,  customer_id  ';

CREATE TABLE t_ws (id int, customer_id int);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_ws'::regclass;
DROP TABLE t_ws;

-- ===== Empty / disabled =====

SET citus.distribution_columns TO '';
CREATE TABLE t_disabled (id int, tenant_id int);
SELECT count(*) FROM citus_tables WHERE table_name = 't_disabled'::regclass;
DROP TABLE t_disabled;

RESET citus.distribution_columns;
CREATE TABLE t_reset (id int, tenant_id int);
SELECT count(*) FROM citus_tables WHERE table_name = 't_reset'::regclass;
DROP TABLE t_reset;

-- ===== Temp tables should NOT be auto-distributed =====

SET citus.distribution_columns TO 'tenant_id';
CREATE TEMP TABLE t_temp (id int, tenant_id int);
-- should not appear in citus_tables (temp tables can't be distributed)
SELECT count(*) FROM citus_tables WHERE table_name = 't_temp'::regclass;
DROP TABLE t_temp;

-- ===== Schema-based sharding takes precedence =====

SET citus.enable_schema_based_sharding TO ON;
SET citus.distribution_columns TO 'tenant_id';

CREATE SCHEMA auto_dist_tenant_schema;
CREATE TABLE auto_dist_tenant_schema.t_tenant (id int, tenant_id int);

-- should be a single-shard (tenant) table, not hash-distributed by tenant_id
SELECT distribution_column FROM citus_tables WHERE table_name = 'auto_dist_tenant_schema.t_tenant'::regclass;

BEGIN;
  SET LOCAL client_min_messages TO WARNING;
  DROP SCHEMA auto_dist_tenant_schema CASCADE;
COMMIT;

RESET citus.enable_schema_based_sharding;

-- ===== NOTICE message shows which column is chosen =====

SET citus.distribution_columns TO 'nonexistent, department';
CREATE TABLE t_notice (id int, department text);
-- The NOTICE should say: auto-distributing table "t_notice" by column "department"
DROP TABLE t_notice;

-- ===== Colocated tables =====

SET citus.distribution_columns TO 'tenant_id';

CREATE TABLE t_coloc1 (id int, tenant_id int);
CREATE TABLE t_coloc2 (id int, tenant_id int);

-- both should be colocated (same distribution column type, same shard count)
SELECT c1.colocation_id = c2.colocation_id AS colocated
FROM citus_tables c1, citus_tables c2
WHERE c1.table_name = 't_coloc1'::regclass
  AND c2.table_name = 't_coloc2'::regclass;

DROP TABLE t_coloc1, t_coloc2;

-- ===== Reference tables: use SET LOCAL to disable GUC temporarily =====

SET citus.distribution_columns TO 'tenant_id';

-- A table with a matching column gets auto-distributed as hash
CREATE TABLE lookup_bad (id int, tenant_id int, name text);
-- This would fail because table is already distributed:
-- SELECT create_reference_table('lookup_bad');
SELECT citus_table_type FROM citus_tables WHERE table_name = 'lookup_bad'::regclass;
DROP TABLE lookup_bad;

-- The correct pattern: use SET LOCAL inside a transaction to temporarily
-- disable the GUC, then create the reference table normally
BEGIN;
  SET LOCAL citus.distribution_columns TO '';
  CREATE TABLE lookup_ref (id int, tenant_id int, name text);
  SELECT create_reference_table('lookup_ref');
COMMIT;

-- verify it's a reference table, not hash-distributed
SELECT citus_table_type FROM citus_tables WHERE table_name = 'lookup_ref'::regclass;

-- also works for tables that have no matching column (no GUC conflict)
CREATE TABLE no_match_ref (id int, code text);
-- no matching column → table is local, so we can make it a reference table
SELECT create_reference_table('no_match_ref');
SELECT citus_table_type FROM citus_tables WHERE table_name = 'no_match_ref'::regclass;

DROP TABLE lookup_ref, no_match_ref;

-- ===== Partitioned tables: parent auto-distributed, partitions follow =====

SET citus.distribution_columns TO 'tenant_id';

-- Range-partitioned table
CREATE TABLE orders (
    id int,
    tenant_id int,
    order_date date,
    amount numeric
) PARTITION BY RANGE (order_date);

-- parent should be auto-distributed by tenant_id
SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'orders'::regclass;

-- create partitions — they should inherit the distribution from the parent
CREATE TABLE orders_2024 PARTITION OF orders
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE orders_2025 PARTITION OF orders
    FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- partitions should also be distributed by tenant_id
SELECT p.table_name::text, distribution_column
FROM citus_tables p
WHERE p.table_name::text IN ('orders_2024', 'orders_2025')
ORDER BY p.table_name::text;

-- insert data and verify it goes to the right partitions
INSERT INTO orders VALUES (1, 10, '2024-06-15', 100.00);
INSERT INTO orders VALUES (2, 20, '2025-03-01', 200.00);
SELECT count(*) FROM orders;
SELECT count(*) FROM orders_2024;
SELECT count(*) FROM orders_2025;

DROP TABLE orders;

-- List-partitioned table
CREATE TABLE events (
    id int,
    tenant_id int,
    event_type text,
    payload text
) PARTITION BY LIST (event_type);

SELECT distribution_column FROM citus_tables WHERE table_name = 'events'::regclass;

CREATE TABLE events_click PARTITION OF events FOR VALUES IN ('click');
CREATE TABLE events_view PARTITION OF events FOR VALUES IN ('view');

SELECT p.table_name::text, distribution_column
FROM citus_tables p
WHERE p.table_name::text IN ('events_click', 'events_view')
ORDER BY p.table_name::text;

INSERT INTO events VALUES (1, 10, 'click', 'data1'), (2, 20, 'view', 'data2');
SELECT count(*) FROM events;

DROP TABLE events;

-- Hash-partitioned table
CREATE TABLE metrics (
    id int,
    tenant_id int,
    metric_name text,
    value float
) PARTITION BY HASH (id);

SELECT distribution_column FROM citus_tables WHERE table_name = 'metrics'::regclass;

CREATE TABLE metrics_p0 PARTITION OF metrics FOR VALUES WITH (MODULUS 2, REMAINDER 0);
CREATE TABLE metrics_p1 PARTITION OF metrics FOR VALUES WITH (MODULUS 2, REMAINDER 1);

SELECT p.table_name::text, distribution_column
FROM citus_tables p
WHERE p.table_name::text IN ('metrics_p0', 'metrics_p1')
ORDER BY p.table_name::text;

DROP TABLE metrics;

-- ===== Partitioned table with no matching column stays local =====

CREATE TABLE local_partitioned (
    id int,
    created_at date
) PARTITION BY RANGE (created_at);

-- no tenant_id column → should NOT be distributed
SELECT count(*) FROM citus_tables WHERE table_name = 'local_partitioned'::regclass;

CREATE TABLE local_partitioned_2024 PARTITION OF local_partitioned
    FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

SELECT count(*) FROM citus_tables WHERE table_name = 'local_partitioned_2024'::regclass;

DROP TABLE local_partitioned;

-- ===== ATTACH PARTITION to an already auto-distributed table =====

CREATE TABLE sales (
    id int,
    tenant_id int,
    sale_date date
) PARTITION BY RANGE (sale_date);

-- auto-distributed
SELECT distribution_column FROM citus_tables WHERE table_name = 'sales'::regclass;

-- create a standalone table, then attach it as a partition
RESET citus.distribution_columns;
CREATE TABLE sales_2026 (id int, tenant_id int, sale_date date);
-- not distributed yet
SELECT count(*) FROM citus_tables WHERE table_name = 'sales_2026'::regclass;

SET citus.distribution_columns TO 'tenant_id';
ALTER TABLE sales ATTACH PARTITION sales_2026 FOR VALUES FROM ('2026-01-01') TO ('2027-01-01');

-- now it should be distributed as part of the parent
SELECT distribution_column FROM citus_tables WHERE table_name = 'sales_2026'::regclass;

DROP TABLE sales;

-- ===== Views should NOT be auto-distributed =====

CREATE TABLE base_for_view (id int, tenant_id int, val text);
-- base table gets distributed
SELECT distribution_column FROM citus_tables WHERE table_name = 'base_for_view'::regclass;

CREATE VIEW v_base AS SELECT * FROM base_for_view;
-- views are not in citus_tables
SELECT count(*) FROM citus_tables WHERE table_name = 'v_base'::regclass;

DROP VIEW v_base;
DROP TABLE base_for_view;

-- ===== IF NOT EXISTS on an already-distributed table =====

CREATE TABLE t_ifne (id int, tenant_id int);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_ifne'::regclass;

-- should not error, just skip
CREATE TABLE IF NOT EXISTS t_ifne (id int, tenant_id int);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_ifne'::regclass;

DROP TABLE t_ifne;

-- ===== Multiple tables in sequence (different distribution columns) =====

SET citus.distribution_columns TO 'org_id, tenant_id';

CREATE TABLE by_org (id int, org_id int);
CREATE TABLE by_tenant (id int, tenant_id int);
CREATE TABLE by_org_and_tenant (id int, org_id int, tenant_id int);

-- org_id wins for table that has both
SELECT table_name::text, distribution_column
FROM citus_tables
WHERE table_name::text IN ('by_org', 'by_tenant', 'by_org_and_tenant')
ORDER BY table_name::text;

DROP TABLE by_org, by_tenant, by_org_and_tenant;

-- ===== Foreign tables should NOT be auto-distributed =====

SET citus.distribution_columns TO 'tenant_id';

CREATE FOREIGN TABLE t_foreign (id int, tenant_id int)
    SERVER fake_fdw_server;
-- foreign tables cannot be hash-distributed, should be skipped
SELECT count(*) FROM citus_tables WHERE table_name = 't_foreign'::regclass;
DROP FOREIGN TABLE t_foreign;

-- ===== Unlogged tables SHOULD be auto-distributed =====

CREATE UNLOGGED TABLE t_unlogged (id int, tenant_id int);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_unlogged'::regclass;
DROP TABLE t_unlogged;

-- ===== Materialized views should NOT be auto-distributed =====

-- create a source table first (reset GUC to avoid auto-distribution)
RESET citus.distribution_columns;
CREATE TABLE matview_source (id int, tenant_id int, val text);
SELECT create_distributed_table('matview_source', 'tenant_id');
INSERT INTO matview_source VALUES (1, 10, 'a'), (2, 20, 'b');

SET citus.distribution_columns TO 'tenant_id';

CREATE MATERIALIZED VIEW mv_test AS SELECT * FROM matview_source;
-- matviews should NOT appear in citus_tables
SELECT count(*) FROM citus_tables WHERE table_name = 'mv_test'::regclass;

DROP MATERIALIZED VIEW mv_test;
DROP TABLE matview_source;

-- ===== SELECT INTO (another form of CTAS) =====

RESET citus.distribution_columns;
CREATE TABLE select_into_source (id int, tenant_id int, data text);
SELECT create_distributed_table('select_into_source', 'tenant_id');
INSERT INTO select_into_source VALUES (1, 10, 'x'), (2, 20, 'y');

SET citus.distribution_columns TO 'tenant_id';

SELECT * INTO t_select_into FROM select_into_source;
-- should be auto-distributed by tenant_id
SELECT distribution_column FROM citus_tables WHERE table_name = 't_select_into'::regclass;
SELECT count(*) FROM t_select_into;

DROP TABLE t_select_into;
DROP TABLE select_into_source;

-- ===== CREATE TABLE ... LIKE =====

RESET citus.distribution_columns;
CREATE TABLE template_table (id int, tenant_id int, name text, created_at timestamptz DEFAULT now());

SET citus.distribution_columns TO 'tenant_id';

CREATE TABLE t_like (LIKE template_table INCLUDING ALL);
-- should be auto-distributed by tenant_id (inherited from LIKE)
SELECT distribution_column FROM citus_tables WHERE table_name = 't_like'::regclass;

DROP TABLE t_like;
DROP TABLE template_table;

-- ===== Table inheritance (INHERITS) should NOT be auto-distributed =====

RESET citus.distribution_columns;
CREATE TABLE parent_inherit (id int, tenant_id int);

SET citus.distribution_columns TO 'tenant_id';

CREATE TABLE child_inherit (extra text) INHERITS (parent_inherit);
-- Citus doesn't support distributing tables with inheritance, should be skipped
SELECT count(*) FROM citus_tables WHERE table_name = 'child_inherit'::regclass;
-- parent with children should also not be auto-distributed
SELECT count(*) FROM citus_tables WHERE table_name = 'parent_inherit'::regclass;

DROP TABLE child_inherit;
DROP TABLE parent_inherit;

-- ===== Non-hashable distribution column type (jsonb) =====

-- In PG 18+, jsonb has a hash function, so it CAN be distributed.
-- This verifies auto-distribution works with non-trivial column types.
CREATE TABLE t_jsonb (id int, tenant_id jsonb);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_jsonb'::regclass;
DROP TABLE t_jsonb;

-- ===== Quoted / case-sensitive column names =====

-- GUC value 'Tenant_Id' is stored as-is; PG stores unquoted column names lowercased
CREATE TABLE t_case1 (id int, tenant_id int);
-- 'tenant_id' in GUC matches 'tenant_id' (stored lowercase) → distributed
SELECT distribution_column FROM citus_tables WHERE table_name = 't_case1'::regclass;
DROP TABLE t_case1;

-- Quoted column name preserves case
CREATE TABLE t_case2 (id int, "Tenant_Id" int);
-- GUC 'tenant_id' does NOT match "Tenant_Id" → should NOT be distributed
SELECT count(*) FROM citus_tables WHERE table_name = 't_case2'::regclass;
DROP TABLE t_case2;

-- But if GUC matches exactly the quoted name
SET citus.distribution_columns TO 'Tenant_Id';
CREATE TABLE t_case3 (id int, "Tenant_Id" int);
-- GUC 'Tenant_Id' matches column "Tenant_Id" → distributed
SELECT distribution_column FROM citus_tables WHERE table_name = 't_case3'::regclass;
DROP TABLE t_case3;

SET citus.distribution_columns TO 'tenant_id';

-- ===== UNIQUE constraint without distribution column → error on CREATE =====

-- UNIQUE on non-distribution column causes auto-distribution to fail,
-- which rolls back the entire CREATE TABLE statement
CREATE TABLE t_unique_bad (id int UNIQUE, tenant_id int);
-- Should error: cannot create constraint ... that does not include partition column

-- UNIQUE including the distribution column → should succeed
CREATE TABLE t_unique_good (id int, tenant_id int, UNIQUE(tenant_id, id));
SELECT distribution_column FROM citus_tables WHERE table_name = 't_unique_good'::regclass;
DROP TABLE t_unique_good;

-- ===== Transaction rollback: auto-distributed table should not persist =====

BEGIN;
  CREATE TABLE t_rollback (id int, tenant_id int);
  -- should exist inside the transaction
  SELECT distribution_column FROM citus_tables WHERE table_name = 't_rollback'::regclass;
ROLLBACK;

-- should not exist after rollback
SELECT count(*) FROM pg_class WHERE relname = 't_rollback';

-- ===== Transaction commit: auto-distributed table should persist =====

BEGIN;
  CREATE TABLE t_commit (id int, tenant_id int);
  SELECT distribution_column FROM citus_tables WHERE table_name = 't_commit'::regclass;
COMMIT;

SELECT distribution_column FROM citus_tables WHERE table_name = 't_commit'::regclass;
DROP TABLE t_commit;

-- ===== Multiple tables in one transaction =====

BEGIN;
  CREATE TABLE t_txn1 (id int, tenant_id int);
  CREATE TABLE t_txn2 (id int, tenant_id int);
COMMIT;

SELECT table_name::text, distribution_column
FROM citus_tables
WHERE table_name::text IN ('t_txn1', 't_txn2')
ORDER BY table_name::text;

DROP TABLE t_txn1, t_txn2;

-- ===== Interaction with citus.use_citus_managed_tables =====

SET citus.use_citus_managed_tables TO ON;
SET citus.distribution_columns TO 'tenant_id';

-- table with matching column → auto-distributed (distribution_columns wins)
CREATE TABLE t_guc_interact1 (id int, tenant_id int);
SELECT citus_table_type, distribution_column
FROM citus_tables WHERE table_name = 't_guc_interact1'::regclass;

-- table without matching column → becomes citus managed table
CREATE TABLE t_guc_interact2 (id int, other_col text);
SELECT citus_table_type
FROM citus_tables WHERE table_name = 't_guc_interact2'::regclass;

DROP TABLE t_guc_interact1, t_guc_interact2;
RESET citus.use_citus_managed_tables;

-- ===== Table in non-public schema =====

CREATE SCHEMA auto_dist_test_schema;

CREATE TABLE auto_dist_test_schema.t_schema (id int, tenant_id int);
SELECT distribution_column FROM citus_tables
WHERE table_name = 'auto_dist_test_schema.t_schema'::regclass;

DROP TABLE auto_dist_test_schema.t_schema;
DROP SCHEMA auto_dist_test_schema;

-- ===== ALTER TABLE ADD COLUMN should NOT retroactively distribute =====

RESET citus.distribution_columns;
CREATE TABLE t_alter_add (id int, other_col text);
-- not distributed (no GUC, no matching column)
SELECT count(*) FROM citus_tables WHERE table_name = 't_alter_add'::regclass;

SET citus.distribution_columns TO 'tenant_id';
ALTER TABLE t_alter_add ADD COLUMN tenant_id int;
-- should still NOT be distributed (auto-distribution only on CREATE TABLE)
SELECT count(*) FROM citus_tables WHERE table_name = 't_alter_add'::regclass;

DROP TABLE t_alter_add;

-- ===== Empty tokens in GUC list (double comma) =====

SET citus.distribution_columns TO 'nonexistent,,tenant_id';
CREATE TABLE t_double_comma (id int, tenant_id int);
SELECT distribution_column FROM citus_tables WHERE table_name = 't_double_comma'::regclass;
DROP TABLE t_double_comma;

-- =============================================================================
-- CTAS WITH NESTED QUERIES
-- =============================================================================
-- Test CREATE TABLE AS SELECT with various source query patterns to verify
-- auto-distribution works correctly and data is fully preserved.

SET citus.distribution_columns TO 'tenant_id';

-- Setup: create source tables with known data

-- Distributed table WITH tenant_id (matches GUC)
RESET citus.distribution_columns;
CREATE TABLE src_distributed (id int, tenant_id int, val text);
SELECT create_distributed_table('src_distributed', 'tenant_id');
INSERT INTO src_distributed VALUES (1,10,'a'),(2,10,'b'),(3,20,'c'),(4,20,'d'),(5,30,'e');

-- Distributed table WITHOUT tenant_id (no matching GUC column)
CREATE TABLE src_no_match (id int, category_id int, info text);
SELECT create_distributed_table('src_no_match', 'category_id');
INSERT INTO src_no_match VALUES (1,100,'x'),(2,100,'y'),(3,200,'z'),(4,300,'w'),(5,300,'v');

-- Reference table
CREATE TABLE src_ref (code int, label text);
SELECT create_reference_table('src_ref');
INSERT INTO src_ref VALUES (10,'ten'),(20,'twenty'),(30,'thirty');

-- Local (plain) table
CREATE TABLE src_local (id int, tenant_id int, note text);
INSERT INTO src_local VALUES (1,10,'n1'),(2,20,'n2'),(3,30,'n3');

SET citus.distribution_columns TO 'tenant_id';

-- ----- CTAS from a nested join query -----
CREATE TABLE ctas_join AS (
    SELECT d.id, d.tenant_id, d.val, r.label
    FROM src_distributed d
    JOIN src_ref r ON d.tenant_id = r.code
);

SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_join'::regclass;

-- row count must match and be non-empty
SELECT count(*) AS join_count FROM ctas_join;
SELECT (SELECT count(*) FROM ctas_join) =
       (SELECT count(*) FROM src_distributed d JOIN src_ref r ON d.tenant_id = r.code)
       AS counts_match;

DROP TABLE ctas_join;

-- ----- CTAS from a distributed table that does NOT have the GUC column -----
-- src_no_match has (id, category_id, info) — no tenant_id column
CREATE TABLE ctas_no_match AS (
    SELECT id, category_id, info FROM src_no_match
);

-- no matching column → should NOT be auto-distributed
SELECT count(*) AS is_distributed FROM citus_tables WHERE table_name = 'ctas_no_match'::regclass;

-- data must still be complete
SELECT count(*) AS no_match_count FROM ctas_no_match;
SELECT (SELECT count(*) FROM ctas_no_match) =
       (SELECT count(*) FROM src_no_match)
       AS counts_match;

DROP TABLE ctas_no_match;

-- ----- CTAS from a local table -----
CREATE TABLE ctas_from_local AS (
    SELECT id, tenant_id, note FROM src_local
);

-- has tenant_id → should be auto-distributed
SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_from_local'::regclass;

SELECT count(*) AS local_count FROM ctas_from_local;
SELECT (SELECT count(*) FROM ctas_from_local) =
       (SELECT count(*) FROM src_local)
       AS counts_match;

DROP TABLE ctas_from_local;

-- ----- CTAS from a distributed table with the same distribution column -----
CREATE TABLE ctas_same_dist AS (
    SELECT id, tenant_id, val FROM src_distributed
);

-- auto-distributed by tenant_id (same as source)
SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_same_dist'::regclass;

SELECT count(*) AS same_dist_count FROM ctas_same_dist;
SELECT (SELECT count(*) FROM ctas_same_dist) =
       (SELECT count(*) FROM src_distributed)
       AS counts_match;

DROP TABLE ctas_same_dist;

-- ----- CTAS from a reference table -----
CREATE TABLE ctas_from_ref AS (
    SELECT code AS tenant_id, label FROM src_ref
);

-- has tenant_id (aliased from code) → should be auto-distributed
SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_from_ref'::regclass;

SELECT count(*) AS ref_count FROM ctas_from_ref;
SELECT (SELECT count(*) FROM ctas_from_ref) =
       (SELECT count(*) FROM src_ref)
       AS counts_match;

DROP TABLE ctas_from_ref;

-- ----- CTAS from a multi-table nested subquery with aggregation -----
CREATE TABLE ctas_nested_agg AS (
    SELECT sub.tenant_id, sub.total_val, r.label
    FROM (
        SELECT tenant_id, count(*) AS total_val
        FROM src_distributed
        GROUP BY tenant_id
    ) sub
    JOIN src_ref r ON sub.tenant_id = r.code
);

SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_nested_agg'::regclass;

SELECT count(*) AS nested_agg_count FROM ctas_nested_agg;
SELECT (SELECT count(*) FROM ctas_nested_agg) > 0 AS is_non_empty;

DROP TABLE ctas_nested_agg;

-- =============================================================================
-- EXPLAIN CREATE TABLE AS SELECT — plan pushdown analysis
-- =============================================================================
-- PostgreSQL supports EXPLAIN CREATE TABLE AS SELECT — it shows the plan
-- for the SELECT without actually creating the table. With Citus, this shows
-- whether the query is pushed down to workers or pulled to coordinator.
--
-- Note: EXPLAIN doesn't trigger auto-distribution (no table is created),
-- so we first EXPLAIN the CTAS to see the plan, then execute the actual
-- CTAS and verify the result.

SET citus.distribution_columns TO 'tenant_id';

-- Setup source tables
RESET citus.distribution_columns;
CREATE TABLE explain_src (id int, tenant_id int, val text);
SELECT create_distributed_table('explain_src', 'tenant_id');
INSERT INTO explain_src VALUES (1,10,'a'),(2,20,'b'),(3,30,'c');
SET citus.distribution_columns TO 'tenant_id';

-- Case 1: CTAS from a distributed table with the SAME distribution column
-- The source table is distributed by tenant_id, the new table will also
-- be auto-distributed by tenant_id → same colocation group

EXPLAIN (COSTS FALSE) CREATE TABLE ctas_explain_same AS
  SELECT * FROM explain_src;

-- Now actually create it and verify
CREATE TABLE ctas_explain_same AS
  SELECT * FROM explain_src;

SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_explain_same'::regclass;

SELECT a.colocation_id = b.colocation_id AS colocated
FROM citus_tables a, citus_tables b
WHERE a.table_name = 'explain_src'::regclass
  AND b.table_name = 'ctas_explain_same'::regclass;

SELECT count(*) AS row_count FROM ctas_explain_same;
SELECT (SELECT count(*) FROM ctas_explain_same) =
       (SELECT count(*) FROM explain_src) AS counts_match;

DROP TABLE ctas_explain_same;

-- Case 2: CTAS from a table with a DIFFERENT distribution column
-- Source is distributed by category_id with 3 shards. The new table gets
-- tenant_id from an alias, making it auto-distributed by tenant_id with
-- 4 shards. EXPLAIN shows the SELECT plan scanning the source (3 shards).
-- After creation, the new table is NOT co-located with the source
-- (different shard count and distribution column).

RESET citus.distribution_columns;
SET citus.shard_count TO 3;  -- different shard count to guarantee non-colocation
CREATE TABLE explain_src_diff (id int, category_id int, val text);
SELECT create_distributed_table('explain_src_diff', 'category_id');
INSERT INTO explain_src_diff VALUES (1,100,'x'),(2,200,'y'),(3,300,'z');
SET citus.shard_count TO 4;
SET citus.distribution_columns TO 'tenant_id';

-- EXPLAIN the CTAS: shows the SELECT plan scanning source with 3 shards
EXPLAIN (COSTS FALSE) CREATE TABLE ctas_explain_diff AS
  SELECT id, category_id AS tenant_id, val FROM explain_src_diff;

-- Now actually create it
CREATE TABLE ctas_explain_diff AS
  SELECT id, category_id AS tenant_id, val FROM explain_src_diff;

SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_explain_diff'::regclass;

-- NOT co-located with source table (different shard count and column)
SELECT a.colocation_id = b.colocation_id AS colocated
FROM citus_tables a, citus_tables b
WHERE a.table_name = 'explain_src_diff'::regclass
  AND b.table_name = 'ctas_explain_diff'::regclass;

SELECT count(*) AS row_count FROM ctas_explain_diff;
SELECT (SELECT count(*) FROM ctas_explain_diff) =
       (SELECT count(*) FROM explain_src_diff) AS counts_match;

DROP TABLE ctas_explain_diff;

-- Case 3: CTAS from a JOIN between two co-located distributed tables
CREATE TABLE explain_items (id int, tenant_id int, qty int);
INSERT INTO explain_items VALUES (1,10,5),(2,20,10),(3,30,15);

-- Both explain_src and explain_items are distributed by tenant_id
SELECT a.colocation_id = b.colocation_id AS colocated
FROM citus_tables a, citus_tables b
WHERE a.table_name = 'explain_src'::regclass
  AND b.table_name = 'explain_items'::regclass;

-- EXPLAIN the CTAS with a co-located join → should push down
EXPLAIN (COSTS FALSE) CREATE TABLE ctas_explain_join AS
  SELECT s.id, s.tenant_id, s.val, i.qty
  FROM explain_src s JOIN explain_items i ON s.tenant_id = i.tenant_id;

CREATE TABLE ctas_explain_join AS
  SELECT s.id, s.tenant_id, s.val, i.qty
  FROM explain_src s JOIN explain_items i ON s.tenant_id = i.tenant_id;

SELECT distribution_column, citus_table_type
FROM citus_tables WHERE table_name = 'ctas_explain_join'::regclass;

SELECT count(*) AS row_count FROM ctas_explain_join;
SELECT (SELECT count(*) FROM ctas_explain_join) =
       (SELECT count(*)
        FROM explain_src s JOIN explain_items i ON s.tenant_id = i.tenant_id)
       AS counts_match;

DROP TABLE ctas_explain_join, explain_items;
DROP TABLE explain_src, explain_src_diff;

-- Cleanup CTAS source tables
DROP TABLE src_distributed, src_no_match, src_ref, src_local;

-- ===== Cleanup =====
RESET citus.distribution_columns;
RESET citus.shard_count;
RESET citus.shard_replication_factor;

SELECT citus_remove_node('localhost', :worker_1_port);
