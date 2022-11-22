CREATE OR REPLACE FUNCTION pg_catalog.citus_get_transaction_clock()
    RETURNS pg_catalog.cluster_clock
    LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT
    AS 'MODULE_PATHNAME',$$citus_get_transaction_clock$$;
COMMENT ON FUNCTION pg_catalog.citus_get_transaction_clock()
    IS 'Returns a transaction timestamp logical clock';
