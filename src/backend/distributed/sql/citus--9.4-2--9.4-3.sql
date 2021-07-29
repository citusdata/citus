-- 9.4-2--9.4-3 was added later as a patch to improve master_update_table_statistics
CREATE OR REPLACE FUNCTION master_update_table_statistics(relation regclass)
RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_update_table_statistics$$;
COMMENT ON FUNCTION pg_catalog.master_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table';
