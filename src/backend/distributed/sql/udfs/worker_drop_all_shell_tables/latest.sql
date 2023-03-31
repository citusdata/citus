 -- During metadata sync, when we send many ddls over single transaction, worker node can error due
-- to reaching at max allocation block size for invalidation messages. To find a workaround for the problem,
-- we added nontransactional metadata sync mode where we create many transaction while dropping shell tables
-- via https://github.com/citusdata/citus/pull/6728.
CREATE OR REPLACE PROCEDURE pg_catalog.worker_drop_all_shell_tables(singleTransaction bool DEFAULT true)
LANGUAGE plpgsql
AS $$
DECLARE
    table_name text;
BEGIN
    -- drop shell tables within single or multiple transactions according to the flag singleTransaction
    FOR table_name IN SELECT logicalrelid::regclass::text FROM pg_dist_partition
    LOOP
        PERFORM pg_catalog.worker_drop_shell_table(table_name);
        IF not singleTransaction THEN
            COMMIT;
        END IF;
    END LOOP;
END;
$$;
COMMENT ON PROCEDURE worker_drop_all_shell_tables(singleTransaction bool)
    IS 'drop all distributed tables only without the metadata within single transaction or '
        'multiple transaction specified by the flag singleTransaction';
