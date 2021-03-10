CREATE OR REPLACE FUNCTION pg_catalog.citus_update_table_statistics(relation regclass)
	RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_update_table_statistics$$;
COMMENT ON FUNCTION pg_catalog.citus_update_table_statistics(regclass)
	IS 'updates shard statistics of the given table';
