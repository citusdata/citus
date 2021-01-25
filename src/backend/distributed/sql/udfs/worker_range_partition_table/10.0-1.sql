CREATE OR REPLACE FUNCTION pg_catalog.worker_range_partition_table_v2(bigint, integer, text, integer, oid, anyarray)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_range_partition_table_v2$$;
COMMENT ON FUNCTION pg_catalog.worker_range_partition_table_v2(bigint, integer, text, integer, oid,
                                                 anyarray)
    IS 'range partition query results';
