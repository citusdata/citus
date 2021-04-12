CREATE OR REPLACE FUNCTION pg_catalog.worker_partitioned_relation_size(text)
    RETURNS bigint
    AS 'MODULE_PATHNAME', $$worker_partitioned_relation_size$$
    LANGUAGE C STRICT VOLATILE;
COMMENT ON FUNCTION pg_catalog.worker_partitioned_relation_size(text)
    IS 'Calculates and returns the size of a partitioned relation';
