-- worker_drop_shell_table_edges drops the cross-table "drop edges" among shell
-- tables so that the shell tables can afterwards be dropped concurrently over the
-- metadata-sync connection pool without deadlocking. Two shell tables that share a
-- drop edge (a foreign key between them, or a partition parent/child relationship)
-- take locks on each other when dropped, so dropping them from two pool connections
-- at once can deadlock. This procedure removes those edges up front, serially, on
-- the worker:
--   (1) DROP every foreign-key constraint that has either endpoint in a shell table;
--   (2) DETACH every partition whose parent is a shell table.
-- After this runs, every shell table is standalone and the parallel drop phase can
-- treat them as independent.
--
-- The ALTERs must not propagate: this is a worker-local teardown, and metadata sync
-- recreates the foreign keys / attachments later in its inter-table phase. We keep
-- citus.enable_ddl_propagation off throughout, re-applying it after each COMMIT
-- (SET LOCAL is scoped to a transaction, so it is lost at COMMIT).
--
-- Locks are bounded by committing every commit_batch_size operations, mirroring the
-- nontransactional worker_drop_all_shell_tables loop, so a cluster with millions of
-- edges does not exhaust max_locks_per_transaction in one giant transaction.
CREATE OR REPLACE PROCEDURE pg_catalog.worker_drop_shell_table_edges(commit_batch_size int DEFAULT 1000)
LANGUAGE plpgsql
AS $$
DECLARE
    fk record;
    part record;
    ops int := 0;
BEGIN
    SET LOCAL citus.enable_ddl_propagation TO 'off';

    -- (1) drop foreign-key constraints touching any shell table (either endpoint)
    FOR fk IN
        SELECT conrelid::regclass::text AS rel, quote_ident(conname) AS conname
        FROM pg_catalog.pg_constraint
        WHERE contype = 'f'
          AND (conrelid IN (SELECT logicalrelid FROM pg_catalog.pg_dist_partition)
               OR confrelid IN (SELECT logicalrelid FROM pg_catalog.pg_dist_partition))
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %s', fk.rel, fk.conname);
        ops := ops + 1;
        IF ops % commit_batch_size = 0 THEN
            COMMIT;
            SET LOCAL citus.enable_ddl_propagation TO 'off';
        END IF;
    END LOOP;

    -- (2) detach partitions whose parent is a shell table
    FOR part IN
        SELECT inhparent::regclass::text AS parent, inhrelid::regclass::text AS child
        FROM pg_catalog.pg_inherits
        WHERE inhparent IN (SELECT logicalrelid FROM pg_catalog.pg_dist_partition)
    LOOP
        EXECUTE format('ALTER TABLE %s DETACH PARTITION %s', part.parent, part.child);
        ops := ops + 1;
        IF ops % commit_batch_size = 0 THEN
            COMMIT;
            SET LOCAL citus.enable_ddl_propagation TO 'off';
        END IF;
    END LOOP;
END;
$$;
COMMENT ON PROCEDURE pg_catalog.worker_drop_shell_table_edges(int)
    IS 'drop foreign-key constraints and detach partitions among shell tables so they '
       'can be dropped concurrently, committing every commit_batch_size operations';
