CREATE SCHEMA publication_partition_root;
SET search_path TO publication_partition_root;
SET citus.shard_replication_factor TO 1;

CREATE FUNCTION activate_node_snapshot()
    RETURNS text[]
    LANGUAGE C STRICT
    AS 'citus';

CREATE TABLE parent_with_children (id int) PARTITION BY RANGE (id);
CREATE TABLE child_1 PARTITION OF parent_with_children
    FOR VALUES FROM (0) TO (10);
CREATE TABLE child_2 PARTITION OF parent_with_children
    FOR VALUES FROM (10) TO (20);
SELECT create_distributed_table('parent_with_children', 'id');

CREATE TABLE parent_without_children (id int) PARTITION BY RANGE (id);
SELECT create_distributed_table('parent_without_children', 'id');

CREATE TABLE standalone (id int);
SELECT create_distributed_table('standalone', 'id');

CREATE SCHEMA "Quoted Schema";
CREATE TABLE "Quoted Schema"."MixedCase Table" (id int);
SELECT create_distributed_table('"Quoted Schema"."MixedCase Table"', 'id');

CREATE PUBLICATION pub_partition_root_with_children
    FOR TABLE parent_with_children
    WITH (publish_via_partition_root = false);
CREATE PUBLICATION pub_partition_root_without_children
    FOR TABLE parent_without_children
    WITH (publish_via_partition_root = false);
CREATE PUBLICATION pub_partition_root_true
    FOR TABLE parent_with_children
    WITH (publish_via_partition_root = true);
CREATE PUBLICATION pub_partition_root_leaf
    FOR TABLE child_1
    WITH (publish_via_partition_root = true);
CREATE PUBLICATION pub_partition_root_mixed
    FOR TABLE parent_with_children, standalone;
CREATE PUBLICATION pub_partition_root_quoted
    FOR TABLE "Quoted Schema"."MixedCase Table";

-- Worker catalogs should contain the exact explicitly published relations.
SELECT DISTINCT result
FROM run_command_on_workers($$
    SELECT array_agg(format('%s:%I.%I:%s',
                            p.pubname, n.nspname, c.relname, p.pubviaroot)
                     ORDER BY p.pubname, n.nspname, c.relname)::text
    FROM pg_publication p
    JOIN pg_publication_rel pr ON pr.prpubid = p.oid
    JOIN pg_class c ON c.oid = pr.prrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE p.pubname LIKE 'pub_partition_root_%'
$$)
ORDER BY result;

-- PostgreSQL should retain its runtime expansion semantics.
SELECT p.pubname,
       COALESCE(string_agg(format('%I.%I', t.schemaname, t.tablename), ', '
                           ORDER BY t.schemaname, t.tablename)
                FILTER (WHERE t.tablename IS NOT NULL), '<none>') AS tables
FROM pg_publication p
LEFT JOIN pg_publication_tables t USING (pubname)
WHERE p.pubname LIKE 'pub_partition_root_%'
GROUP BY p.pubname
ORDER BY p.pubname;

-- Node activation should reconstruct the roots, not expanded leaves.
SELECT c
FROM unnest(activate_node_snapshot()) c
WHERE c LIKE '%CREATE PUBLICATION%'
  AND c LIKE '%pub_partition_root_%'
ORDER BY c;

DROP PUBLICATION pub_partition_root_with_children;
DROP PUBLICATION pub_partition_root_without_children;
DROP PUBLICATION pub_partition_root_true;
DROP PUBLICATION pub_partition_root_leaf;
DROP PUBLICATION pub_partition_root_mixed;
DROP PUBLICATION pub_partition_root_quoted;
SET client_min_messages TO ERROR;
DROP SCHEMA publication_partition_root CASCADE;
DROP SCHEMA "Quoted Schema" CASCADE;
RESET client_min_messages;

SELECT public.wait_for_resource_cleanup();
