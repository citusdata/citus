--
-- METADATA_SYNC_BATCHING
--
-- Tests that syncing metadata to a node works correctly when the
-- coordinator periodically flushes its caches
-- (citus.metadata_sync_cache_flush_interval) and batches per-object
-- metadata into set-based statements (citus.metadata_sync_set_batch_size).
--
-- We create 110 distributed schemas with 2 tables each, and a foreign key
-- from one table to the other in each schema. With both settings at 50,
-- every metadata sync step that flushes the caches or batches the metadata
-- processes more than 100 objects. So each of them flushes the caches more
-- than once and sends at least two full batches plus a final partial batch.
--

CREATE SCHEMA metadata_sync_batching;
SET search_path TO metadata_sync_batching;

-- Returns the number of metadata records for the objects in the schemas
-- whose names match schema_pattern, plus the number of foreign keys
-- defined on the Citus tables in those schemas.
CREATE FUNCTION metadata_counts(schema_pattern text)
RETURNS TABLE (metadata text, count bigint)
LANGUAGE sql
AS $func$
    WITH citus_tables AS (
        SELECT p.logicalrelid, p.colocationid
        FROM pg_dist_partition p
        JOIN pg_class c ON c.oid = p.logicalrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname LIKE schema_pattern
    )
    SELECT 'pg_dist_partition', count(*) FROM citus_tables
    UNION ALL
    SELECT 'pg_dist_shard', count(*)
    FROM pg_dist_shard s JOIN citus_tables USING (logicalrelid)
    UNION ALL
    SELECT 'pg_dist_placement', count(*)
    FROM pg_dist_placement pl
    JOIN pg_dist_shard s USING (shardid)
    JOIN citus_tables USING (logicalrelid)
    UNION ALL
    SELECT 'pg_dist_object', count(*)
    FROM pg_dist_object o,
         LATERAL pg_identify_object(o.classid, o.objid, o.objsubid) i
    WHERE i.schema LIKE schema_pattern OR
          (o.classid = 'pg_namespace'::regclass AND
           (SELECT nspname FROM pg_namespace WHERE oid = o.objid) LIKE schema_pattern)
    UNION ALL
    SELECT 'pg_dist_colocation', count(*)
    FROM pg_dist_colocation
    WHERE colocationid IN (SELECT colocationid FROM citus_tables)
    UNION ALL
    SELECT 'pg_dist_schema', count(*)
    FROM pg_dist_schema ds
    JOIN pg_namespace n ON n.oid = ds.schemaid
    WHERE n.nspname LIKE schema_pattern
    UNION ALL
    SELECT 'foreign keys', count(*)
    FROM citus_tables
    JOIN pg_constraint con ON con.conrelid = citus_tables.logicalrelid
    WHERE con.contype = 'f'
$func$;

SET citus.next_shard_id TO 9200000;
SET citus.shard_replication_factor TO 1;

SET citus.metadata_sync_cache_flush_interval TO 50;
SET citus.metadata_sync_set_batch_size TO 50;

-- store the current sequence values to restart them before adding the node back
SELECT nextval('pg_catalog.pg_dist_groupid_seq') - 1 AS last_group_id \gset
SELECT nextval('pg_catalog.pg_dist_node_nodeid_seq') - 1 AS last_node_id \gset

SELECT citus_remove_node('localhost', :worker_2_port);

SET citus.enable_schema_based_sharding TO ON;

-- don't echo the generated commands to keep the output short
\set ECHO none
SELECT format('CREATE SCHEMA msb_t%s', i) FROM generate_series(1, 110) i \gexec
SELECT format('CREATE TABLE msb_t%s.referenced_table (id int PRIMARY KEY)', i)
FROM generate_series(1, 110) i \gexec
SELECT format('CREATE TABLE msb_t%1$s.referencing_table (id int, '
              'ref_id int REFERENCES msb_t%1$s.referenced_table (id))', i)
FROM generate_series(1, 110) i \gexec
\set ECHO all

RESET citus.enable_schema_based_sharding;

-- sanity check the fixture on the coordinator
SELECT * FROM metadata_counts('msb\_t%') ORDER BY metadata;

--
-- Sync the metadata to the node in transactional mode.
--
SET citus.metadata_sync_mode TO 'transactional';

ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

SELECT jsonb_object_agg(metadata, count) AS coordinator_counts
FROM metadata_counts('msb\_t%') \gset

\c - - - :worker_2_port
SET search_path TO metadata_sync_batching;
SELECT metadata,
       (:'coordinator_counts'::jsonb ->> metadata)::bigint AS coordinator,
       count AS worker,
       (:'coordinator_counts'::jsonb ->> metadata)::bigint = count AS equal
FROM metadata_counts('msb\_t%') ORDER BY metadata;

\c - - - :master_port
SET search_path TO metadata_sync_batching;
SET citus.metadata_sync_cache_flush_interval TO 50;
SET citus.metadata_sync_set_batch_size TO 50;

--
-- Sync the metadata to the node in nontransactional mode.
--
SELECT citus_remove_node('localhost', :worker_2_port);

SET citus.metadata_sync_mode TO 'nontransactional';

ALTER SEQUENCE pg_catalog.pg_dist_groupid_seq RESTART :last_group_id;
ALTER SEQUENCE pg_catalog.pg_dist_node_nodeid_seq RESTART :last_node_id;
SELECT 1 FROM citus_add_node('localhost', :worker_2_port);

RESET citus.metadata_sync_mode;

SELECT jsonb_object_agg(metadata, count) AS coordinator_counts
FROM metadata_counts('msb\_t%') \gset

\c - - - :worker_2_port
SET search_path TO metadata_sync_batching;
SELECT metadata,
       (:'coordinator_counts'::jsonb ->> metadata)::bigint AS coordinator,
       count AS worker,
       (:'coordinator_counts'::jsonb ->> metadata)::bigint = count AS equal
FROM metadata_counts('msb\_t%') ORDER BY metadata;

\c - - - :master_port

-- cleanup
SET client_min_messages TO WARNING;
\set ECHO none
SELECT format('DROP SCHEMA msb_t%s CASCADE', i) FROM generate_series(1, 110) i \gexec
\set ECHO all
DROP SCHEMA metadata_sync_batching CASCADE;
