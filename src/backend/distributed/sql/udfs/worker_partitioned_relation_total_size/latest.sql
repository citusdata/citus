CREATE OR REPLACE FUNCTION pg_catalog.worker_partitioned_relation_total_size(text)
    RETURNS bigint
    AS 'MODULE_PATHNAME', $$worker_partitioned_relation_total_size$$
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.worker_partitioned_relation_total_size(text)
    IS 'Calculates and returns the total size of a partitioned relation';
