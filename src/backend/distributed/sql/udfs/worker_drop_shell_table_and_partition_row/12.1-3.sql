-- worker_drop_shell_table_and_partition_row drops one shell table on the worker and,
-- atomically in the same command, deletes its pg_dist_partition row. It is the
-- parallel-drop-phase counterpart of worker_drop_shell_table: metadata sync drives it
-- over the connection pool, one table per task. Deleting the pg_dist_partition row
-- here (rather than in a single bulk DELETE afterwards) is what lets the later
-- safety-net CALL worker_drop_all_shell_tables(false) run over a near-empty
-- pg_dist_partition instead of looping all ~N rows with a COMMIT each.
--
-- Like worker_drop_shell_table it removes the shell table via an internal
-- performDeletion() (so it does not fire the Citus drop event trigger) and is a
-- no-op NOTICE if the relation no longer exists. It deliberately does NOT touch
-- pg_dist_shard, pg_dist_placement or pg_dist_object; those are cleared by the
-- existing bulk metadata-deletion path.
CREATE OR REPLACE FUNCTION pg_catalog.worker_drop_shell_table_and_partition_row(table_name text)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_shell_table_and_partition_row$$;
COMMENT ON FUNCTION pg_catalog.worker_drop_shell_table_and_partition_row(table_name text)
    IS 'drop a shell table and delete its pg_dist_partition row in one command';
