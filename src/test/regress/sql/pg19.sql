--
-- PG19
--
SHOW server_version \gset
SELECT substring(:'server_version', '\d+')::int >= 19 AS server_version_ge_19
\gset
\if :server_version_ge_19
\else
\q
\endif

-- PG19-specific tests go here.
--
-- REPACK / CLUSTER dispatch (#8613): on PG19 the legacy CLUSTER command and the
-- new REPACK command share a single parse node (RepackStmt, aliased back to
-- ClusterStmt in Citus).  Citus propagates REPACK exactly like CLUSTER: the
-- command is shipped to every shard placement.  These tests prove that a
-- distributed REPACK actually reaches and rewrites every shard placement (the
-- relfilenode of each shard changes), that CLUSTER still works through the same
-- shared code path, and that the user-facing messages are command-aware.
--
-- VACUUM FULL is unaffected: core keeps dispatching it through T_VacuumStmt, so
-- it never reaches this CLUSTER/REPACK path.

CREATE SCHEMA pg19_repack;
SET search_path TO pg19_repack;

SET citus.next_shard_id TO 1100000;
SET citus.shard_count TO 4;
SET citus.shard_replication_factor TO 1;

CREATE TABLE repack_test (a int, b int);
SELECT create_distributed_table('repack_test', 'a');
INSERT INTO repack_test SELECT g, g % 10 FROM generate_series(1, 100) g;
CREATE INDEX repack_test_a_idx ON repack_test (a);

-- snapshot the distribution metadata + shard placements BEFORE any repack, so that
-- afterwards we can prove REPACK/CLUSTER rewrite the heaps WITHOUT disturbing the
-- table's distributed nature (its shards, placements and colocation must survive).
CREATE TEMP TABLE dist_before AS
    SELECT sh.shardid, pl.shardstate, n.nodename, n.nodeport
    FROM pg_dist_shard sh
    JOIN pg_dist_placement pl ON pl.shardid = sh.shardid
    JOIN pg_dist_node n ON n.groupid = pl.groupid
    WHERE sh.logicalrelid = 'repack_test'::regclass;

CREATE TEMP TABLE meta_before AS
    SELECT partmethod, partkey, colocationid
    FROM pg_dist_partition
    WHERE logicalrelid = 'repack_test'::regclass;

-- snapshot the per-shard row counts BEFORE any repack, so we can prove afterwards
-- that REPACK/CLUSTER rewrite the heaps WITHOUT moving any row across shards (every
-- shard must keep its exact row set; the hash-mapping of rows to shards is invariant).
CREATE TEMP TABLE rows_before AS
    SELECT shardid, result AS rowcount
    FROM run_command_on_shards('repack_test', $$ SELECT count(*) FROM %s $$);

-- helper: capture the relfilenode of every shard placement.  REPACK and CLUSTER
-- both rewrite the heap, so a changed relfilenode on every placement is a
-- definitive demonstration that the command was dispatched and executed there.

-- REPACK <tbl> USING INDEX <idx> must propagate to every shard placement
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

REPACK repack_test USING INDEX repack_test_a_idx;

SELECT bool_and(after.result <> b.relfilenode) AS all_shards_rewritten
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);

-- the order-by index becomes the clustered index on every shard placement
SELECT bool_and(result::boolean) AS all_shards_clustered
FROM run_command_on_shards('repack_test',
        $$ SELECT EXISTS (SELECT 1 FROM pg_index
                          WHERE indrelid = '%s'::regclass AND indisclustered) $$);

-- bare REPACK <tbl> (no index) also rewrites every shard placement
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

REPACK repack_test;

SELECT bool_and(after.result <> b.relfilenode) AS all_shards_rewritten
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);

-- data is preserved across the rewrites
SELECT count(*) FROM repack_test;

-- CLUSTER <tbl> USING <idx> still works through the same shared code path
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

CLUSTER repack_test USING repack_test_a_idx;

SELECT bool_and(after.result <> b.relfilenode) AS all_shards_rewritten
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);

DROP TABLE IF EXISTS rf_before;

-- The heaps were rewritten on every placement above.  Now prove the table is STILL
-- a correctly distributed Citus table: the shard set is unchanged, every placement is
-- still on its original node and active, and the distribution method/colocation are
-- intact.  (REPACK/CLUSTER must rewrite storage without touching distribution.)
CREATE TEMP TABLE dist_after AS
    SELECT sh.shardid, pl.shardstate, n.nodename, n.nodeport
    FROM pg_dist_shard sh
    JOIN pg_dist_placement pl ON pl.shardid = sh.shardid
    JOIN pg_dist_node n ON n.groupid = pl.groupid
    WHERE sh.logicalrelid = 'repack_test'::regclass;

SELECT NOT EXISTS (
    (TABLE dist_before EXCEPT TABLE dist_after)
    UNION ALL
    (TABLE dist_after EXCEPT TABLE dist_before)
) AS placements_unchanged;

SELECT count(*) = (SELECT count(DISTINCT shardid) FROM dist_before)
       AS shard_count_unchanged
FROM pg_dist_shard WHERE logicalrelid = 'repack_test'::regclass;

-- no row crossed a shard boundary: every shard placement still holds the exact same
-- number of rows it held before the rewrites (proves the hash distribution is intact).
SELECT bool_and(after.result = rb.rowcount) AS shard_row_mapping_unchanged
FROM run_command_on_shards('repack_test', $$ SELECT count(*) FROM %s $$) after
JOIN rows_before rb USING (shardid);

SELECT p.partmethod = m.partmethod
       AND p.partkey IS NOT DISTINCT FROM m.partkey
       AND p.colocationid = m.colocationid
       AS distribution_unchanged
FROM pg_dist_partition p, meta_before m
WHERE p.logicalrelid = 'repack_test'::regclass;

-- distributed query execution still works after the rewrite: router queries prune to
-- a single shard each (b = a % 10), and a cross-shard aggregate runs on every shard.
SELECT a, b FROM repack_test WHERE a = 42;
SELECT a, b FROM repack_test WHERE a = 100;
SELECT count(*) AS rows_total, sum(a) AS sum_a FROM repack_test;

-- command-aware messages: VERBOSE is unsupported for distributed tables, and the
-- error names the actual command (REPACK vs CLUSTER).  Citus raises this in
-- preprocess and aborts, so no shard work happens.
REPACK (VERBOSE) repack_test USING INDEX repack_test_a_idx;
CLUSTER VERBOSE repack_test USING repack_test_a_idx;

-- command-aware messages: the PG19-only REPACK options CONCURRENTLY and ANALYZE are
-- rejected in preprocess, before any shard placement is touched.  CONCURRENTLY relies
-- on PreventInTransactionBlock and can not ride worker_apply_shard_ddl_command;
-- ANALYZE has no per-shard semantics yet.  Prove the rejection is early by snapshotting
-- every shard's relfilenode, running the (failing) commands, and asserting nothing was
-- rewritten.
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

REPACK (CONCURRENTLY) repack_test USING INDEX repack_test_a_idx;
REPACK (ANALYZE) repack_test;

SELECT bool_and(after.result = b.relfilenode) AS no_shard_touched_by_rejected_options
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);
DROP TABLE IF EXISTS rf_before;

-- explicit-off / explicit-false boolean options are DISABLED, not rejected: by
-- PostgreSQL truth-value semantics CONCURRENTLY off and ANALYZE false leave the
-- option unset, so Citus does NOT reject them -- the command proceeds as an
-- ordinary REPACK and is dispatched to every shard placement (relfilenode
-- changes), unlike the enabled CONCURRENTLY/ANALYZE forms rejected above.
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

REPACK (CONCURRENTLY off) repack_test USING INDEX repack_test_a_idx;
REPACK (ANALYZE false) repack_test;

SELECT bool_and(after.result <> b.relfilenode) AS disabled_options_still_repack
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);
DROP TABLE IF EXISTS rf_before;

-- data is preserved by the ordinary REPACKs above
SELECT count(*) AS rows_after_disabled_option_repack FROM repack_test;

-- transaction-block gate: an ENABLED CONCURRENTLY is rejected by Citus in
-- preprocess, before core's PreventInTransactionBlock check or any shard
-- dispatch, so the gate fires even inside an explicit transaction.  Snapshot
-- every shard's relfilenode, run the (failing) command in a transaction, roll
-- back, and assert no placement was touched.
DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

BEGIN;
REPACK (CONCURRENTLY) repack_test USING INDEX repack_test_a_idx;
ROLLBACK;

SELECT bool_and(after.result = b.relfilenode) AS no_shard_touched_in_txn
FROM run_command_on_shards('repack_test',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);
DROP TABLE IF EXISTS rf_before;

-- boundary: VACUUM FULL must NOT reach the REPACK/CLUSTER path.  Core keeps dispatching
-- it via T_VacuumStmt, so Citus' vacuum path runs it and it still succeeds (and preserves
-- the data) on a distributed table.
VACUUM FULL repack_test;
SELECT count(*) AS rows_after_vacuum_full FROM repack_test;

-- quoted / mixed-case relation AND index names: dispatch relabels the parse tree in place
-- (AppendShardIdToName), so REPACK ... USING INDEX must localize BOTH the quoted relation
-- name and the quoted index name on every shard placement.
CREATE TABLE "Repack Quoted" (a int, b int);
SELECT create_distributed_table('"Repack Quoted"', 'a');
INSERT INTO "Repack Quoted" SELECT g, g % 10 FROM generate_series(1, 20) g;
CREATE INDEX "Weird Idx!" ON "Repack Quoted" (a);

DROP TABLE IF EXISTS rf_before;
CREATE TEMP TABLE rf_before AS
    SELECT shardid, result AS relfilenode
    FROM run_command_on_shards('"Repack Quoted"',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$);

REPACK "Repack Quoted" USING INDEX "Weird Idx!";

SELECT bool_and(after.result <> b.relfilenode) AS quoted_all_shards_rewritten
FROM run_command_on_shards('"Repack Quoted"',
        $$ SELECT relfilenode FROM pg_class WHERE oid = '%s'::regclass $$) after
JOIN rf_before b USING (shardid);
DROP TABLE IF EXISTS rf_before;

-- transaction-block behaviour: a non-partitioned distributed REPACK is dispatched through
-- the coordinated 2PC (like CLUSTER), so it commits cleanly inside BEGIN/COMMIT and leaves
-- the data intact.
BEGIN;
REPACK repack_test USING INDEX repack_test_a_idx;
COMMIT;
SELECT count(*) AS rows_after_txn_repack FROM repack_test;

-- command-aware messages: partitioned distributed tables are not propagated (CLUSTER
-- and REPACK can not run inside a transaction block on partitioned tables), and the
-- WARNING must name the actual command (REPACK here, mirroring the CLUSTER case in
-- pg15.sql).  Citus warns and returns NIL, so no shard work happens.
CREATE TABLE repack_part (id int, ts date) PARTITION BY RANGE (ts);
ALTER TABLE repack_part ADD CONSTRAINT repack_part_pk PRIMARY KEY (id, ts);
CREATE TABLE repack_part_2020 PARTITION OF repack_part
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE TABLE repack_part_2021 PARTITION OF repack_part
    FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');
SELECT create_distributed_table('repack_part', 'id');
REPACK repack_part USING INDEX repack_part_pk;

SET client_min_messages TO WARNING;
DROP SCHEMA pg19_repack CASCADE;
RESET client_min_messages;
RESET search_path;
