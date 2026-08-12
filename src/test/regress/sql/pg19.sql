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

-- PG19 represents ALL TABLES EXCEPT relations as PUBLICATIONOBJ_EXCEPT_TABLE,
-- and stores ALL TABLES/ALL SEQUENCES as independent flags. Verify both
-- catalog-backed CREATE reconstruction and direct ALTER propagation.
CREATE TABLE publication_excluded_1 (id int);
CREATE TABLE publication_excluded_2 (id int);
CREATE TABLE publication_local_excluded (id int);
SELECT create_distributed_table('publication_excluded_1', 'id');
SELECT create_distributed_table('publication_excluded_2', 'id');

CREATE PUBLICATION publication_all_except
    FOR ALL TABLES EXCEPT (TABLE publication_excluded_1,
                           TABLE publication_local_excluded),
        ALL SEQUENCES;

SELECT bool_and(result::boolean) AS create_propagated
FROM run_command_on_workers($$
    SELECT p.puballtables AND p.puballsequences
           AND count(*) FILTER (WHERE r.prexcept) = 1
           AND bool_and(c.relname = 'publication_excluded_1')
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid
    JOIN pg_class c ON c.oid = r.prrelid
    WHERE p.pubname = 'publication_all_except'
    GROUP BY p.puballtables, p.puballsequences
$$);

ALTER PUBLICATION publication_all_except
    SET ALL TABLES EXCEPT (TABLE publication_excluded_2,
                           TABLE publication_local_excluded),
        ALL SEQUENCES;

SELECT bool_and(result::boolean) AS alter_propagated
FROM run_command_on_workers($$
    SELECT p.puballtables AND p.puballsequences
           AND count(*) FILTER (WHERE r.prexcept) = 1
           AND bool_and(c.relname = 'publication_excluded_2')
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid
    JOIN pg_class c ON c.oid = r.prrelid
    WHERE p.pubname = 'publication_all_except'
    GROUP BY p.puballtables, p.puballsequences
$$);

DROP PUBLICATION publication_all_except;

-- A table that is excluded while it is still a local table cannot be named on
-- the workers, so it is filtered out when the publication is propagated.
-- Distributing it later has to restore the exclusion on the workers, otherwise
-- the workers would publish a table that the coordinator excludes.
CREATE TABLE publication_late_excluded (id int);

CREATE PUBLICATION publication_all_except_late
    FOR ALL TABLES EXCEPT (TABLE publication_late_excluded);

SELECT bool_and(result::boolean) AS exclusion_deferred_while_local
FROM run_command_on_workers($$
    SELECT count(*) = 0
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
    WHERE p.pubname = 'publication_all_except_late'
$$);

SELECT create_distributed_table('publication_late_excluded', 'id');

SELECT bool_and(result::boolean) AS exclusion_restored_after_distribution
FROM run_command_on_workers($$
    SELECT count(*) = 1
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
    JOIN pg_class c ON c.oid = r.prrelid
    WHERE p.pubname = 'publication_all_except_late'
      AND c.relname = 'publication_late_excluded'
$$);

SELECT count(*) = 1 AS exclusion_retained_on_coordinator
FROM pg_publication p
JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname = 'publication_all_except_late'
  AND c.relname = 'publication_late_excluded';

DROP PUBLICATION publication_all_except_late;

-- Reference-table creation follows the same metadata-sync path and must restore
-- an exclusion that could not be propagated while the table was still local.
CREATE TABLE publication_ref_excluded (id int PRIMARY KEY);
CREATE PUBLICATION publication_all_except_ref
    FOR ALL TABLES EXCEPT (TABLE publication_ref_excluded);

SELECT create_reference_table('publication_ref_excluded');

SELECT bool_and(result::boolean) AS exclusion_restored_after_reference_table
FROM run_command_on_workers($$
    SELECT count(*) = 1
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
    JOIN pg_class c ON c.oid = r.prrelid
    WHERE p.pubname = 'publication_all_except_ref'
      AND c.relname = 'publication_ref_excluded'
$$);

DROP PUBLICATION publication_all_except_ref;
DROP TABLE publication_ref_excluded;

-- The activation path rebuilds publication DDL from catalogs rather than
-- deparsing the original statement, so exercise it directly. An excluded
-- partitioned root must survive as the root (not be expanded to its leaves),
-- and excluded Citus tables must be ordered before the publication itself.
-- The publication is created while the excluded root is still a local table so
-- that it is registered as a distributed object before the table is; only the
-- publication -> excluded relation dependency can order the table first.
CREATE TABLE publication_part_excluded (id int, ts timestamptz)
    PARTITION BY RANGE (ts);
CREATE TABLE publication_part_excluded_2020 PARTITION OF publication_part_excluded
    FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');
CREATE PUBLICATION publication_all_except_part
    FOR ALL TABLES EXCEPT (TABLE publication_part_excluded);

SELECT create_distributed_table('publication_part_excluded', 'id');

CREATE OR REPLACE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';

SELECT count(*) = 1 AS part_root_preserved
FROM unnest(activate_node_snapshot()) c
WHERE c LIKE '%CREATE PUBLICATION%publication_all_except_part%'
  AND c LIKE '%publication_part_excluded%'
  AND c NOT LIKE '%publication_part_excluded_2020%';

SELECT min(ord) FILTER (WHERE c LIKE '%publication_part_excluded%'
                          AND c NOT LIKE '%publication_part_excluded_2020%'
                          AND c NOT LIKE '%CREATE PUBLICATION%')
     < min(ord) FILTER (WHERE c LIKE '%CREATE PUBLICATION%publication_all_except_part%')
       AS excluded_table_precedes_publication
FROM unnest(activate_node_snapshot()) WITH ORDINALITY AS s(c, ord);

DROP PUBLICATION publication_all_except_part;

-- Conversions that give a table a new OID must not lose its exclusion, because
-- the exclusion is tied to the old OID and cannot be restored with
-- ALTER PUBLICATION .. ADD TABLE.
SET client_min_messages TO WARNING;

-- undistribute_table() drops the table and recreates it under the same name.
CREATE TABLE publication_undist_excluded (id int);

CREATE PUBLICATION publication_all_except_undist
    FOR ALL TABLES EXCEPT (TABLE publication_undist_excluded);

SELECT create_distributed_table('publication_undist_excluded', 'id');
SELECT undistribute_table('publication_undist_excluded');

SELECT count(*) = 1 AS exclusion_retained_after_undistribute
FROM pg_publication p
JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname = 'publication_all_except_undist'
  AND c.relname = 'publication_undist_excluded';

DROP PUBLICATION publication_all_except_undist;
DROP TABLE publication_undist_excluded;

-- citus_add_local_table_to_metadata() renames the original table into a shard
-- and creates a new shell table under the original name, so the exclusion has
-- to move to the shell table instead of staying behind on the shard.
SELECT citus_set_coordinator_host('localhost', :master_port);

CREATE TABLE publication_conv_excluded (id int);

CREATE PUBLICATION publication_all_except_conv
    FOR ALL TABLES EXCEPT (TABLE publication_conv_excluded);

SELECT citus_add_local_table_to_metadata('publication_conv_excluded');

SELECT count(*) = 1 AS exclusion_moved_to_shell_table
FROM pg_publication p
JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname = 'publication_all_except_conv'
  AND c.relname = 'publication_conv_excluded';

SELECT count(*) = 0 AS no_exclusion_left_behind_on_shard
FROM pg_publication p
JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
JOIN pg_class c ON c.oid = r.prrelid
WHERE p.pubname = 'publication_all_except_conv'
  AND c.relname <> 'publication_conv_excluded';

SELECT bool_and(result::boolean) AS exclusion_present_on_workers
FROM run_command_on_workers($$
    SELECT count(*) = 1
    FROM pg_publication p
    JOIN pg_publication_rel r ON r.prpubid = p.oid AND r.prexcept
    JOIN pg_class c ON c.oid = r.prrelid
    WHERE p.pubname = 'publication_all_except_conv'
      AND c.relname = 'publication_conv_excluded'
$$);

DROP PUBLICATION publication_all_except_conv;
DROP TABLE publication_conv_excluded;

-- JSON COPY framing and protocol state must be owned by one coordinator COPY.
CREATE TABLE json_copy_dist
(
    id int,
    payload text,
    optional text,
    generated text GENERATED ALWAYS AS (payload) STORED
);
SELECT create_distributed_table('json_copy_dist', 'id',
                                shard_count => 4, colocate_with => 'none');
INSERT INTO json_copy_dist
SELECT id, E'quote" slash\\ newline\n', NULL
FROM (
    SELECT DISTINCT ON (get_shard_id_for_distribution_column('json_copy_dist', id))
           id
    FROM generate_series(1, 100) id
    ORDER BY get_shard_id_for_distribution_column('json_copy_dist', id), id
    LIMIT 2
) distinct_shards;

SELECT count(DISTINCT get_shard_id_for_distribution_column('json_copy_dist', id)) = 2
       AS rows_on_distinct_shards
FROM json_copy_dist;

COPY json_copy_dist (payload, optional) TO STDOUT
    (FORMAT json, FORCE_ARRAY true);
COPY json_copy_dist (payload) TO STDOUT
    (FORMAT json, FORCE_ARRAY false);
COPY json_copy_dist (payload) TO STDOUT
    (FORMAT json, FORCE_ARRAY true, ENCODING 'UTF8', HEADER false);

CREATE TABLE json_copy_empty (id int);
SELECT create_distributed_table('json_copy_empty', 'id',
                                shard_count => 4, colocate_with => 'none');
COPY json_copy_empty TO STDOUT (FORMAT json, FORCE_ARRAY true);

CREATE TABLE json_copy_single_shard (id int, payload text);
SELECT create_distributed_table('json_copy_single_shard', NULL,
                                colocate_with => 'none');
INSERT INTO json_copy_single_shard VALUES (1, 'single shard');
COPY json_copy_single_shard TO STDOUT (FORMAT json, FORCE_ARRAY true);

CREATE TABLE json_copy_reference (id int PRIMARY KEY, payload text);
SELECT create_reference_table('json_copy_reference');
INSERT INTO json_copy_reference VALUES (1, 'reference');
COPY json_copy_reference TO STDOUT (FORMAT json, FORCE_ARRAY true);

CREATE TABLE json_copy_local
(
    id int,
    payload text,
    generated text GENERATED ALWAYS AS (payload) STORED
);
INSERT INTO json_copy_local VALUES (1, 'local');
COPY json_copy_local TO STDOUT (FORMAT json, FORCE_ARRAY true);

COPY json_copy_local (generated) TO STDOUT (FORMAT json);
COPY json_copy_dist (generated) TO STDOUT (FORMAT json);
COPY json_copy_local (ctid) TO STDOUT (FORMAT json);
COPY json_copy_dist (ctid) TO STDOUT (FORMAT json);

CREATE TABLE json_copy_citus_local (id int, payload text);
SELECT citus_add_local_table_to_metadata('json_copy_citus_local');
INSERT INTO json_copy_citus_local VALUES (1, 'Citus local');
COPY json_copy_citus_local TO STDOUT (FORMAT json, FORCE_ARRAY true);

COPY json_copy_dist TO STDOUT (FORMAT json, DELIMITER ',');
COPY json_copy_dist TO STDOUT (FORMAT json, HEADER true);

DROP TABLE json_copy_dist, json_copy_empty, json_copy_single_shard,
           json_copy_reference, json_copy_local, json_copy_citus_local;

SELECT citus_remove_node('localhost', :master_port);
RESET client_min_messages;

SET client_min_messages TO WARNING;
DROP SCHEMA pg19_repack CASCADE;
RESET client_min_messages;
RESET search_path;
