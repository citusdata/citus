DROP FUNCTION pg_catalog.worker_partition_query_result(text, text, int, citus.distribution_type, text[], text[], boolean);

CREATE OR REPLACE FUNCTION pg_catalog.worker_partition_query_result(
    result_prefix text,
    query text,
    partition_column_index int,
    partition_method citus.distribution_type,
    partition_min_values text[],
    partition_max_values text[],
    binary_copy boolean,
    allow_null_partition_column boolean DEFAULT false,
    generate_empty_results boolean DEFAULT false,
    OUT partition_index int,
    OUT rows_written bigint,
    OUT bytes_written bigint)
RETURNS SETOF record
LANGUAGE C STRICT VOLATILE
AS 'MODULE_PATHNAME', $$worker_partition_query_result$$;
COMMENT ON FUNCTION pg_catalog.worker_partition_query_result(text, text, int, citus.distribution_type, text[], text[], boolean, boolean, boolean)
IS 'execute a query and partitions its results in set of local result files';
