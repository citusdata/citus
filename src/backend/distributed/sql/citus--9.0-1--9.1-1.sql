CREATE OR REPLACE FUNCTION pg_catalog.create_hash_partitioned_intermediate_result(
    result_prefix text,
    query text,
    partition_column_index int,
    hash_ranges int[],
    OUT partition_index int,
    OUT bytes_written bigint)
RETURNS SETOF record
LANGUAGE C STRICT VOLATILE
AS 'MODULE_PATHNAME', $$create_hash_partitioned_intermediate_result$$;
COMMENT ON FUNCTION pg_catalog.create_hash_partitioned_intermediate_result(result_prefix text, query text, partition_column_index int, hash_ranges int[])
IS 'execute a query and partitions its results in set of local result files';

CREATE OR REPLACE FUNCTION pg_catalog.fetch_intermediate_results(
    result_prefixes text[],
    node_name text,
    node_port int)
RETURNS bigint
LANGUAGE C STRICT VOLATILE
AS 'MODULE_PATHNAME', $$fetch_intermediate_results$$;
COMMENT ON FUNCTION pg_catalog.fetch_intermediate_results(text[],text,int)
IS 'fetch an intermediate result from a remote node';

CREATE OR REPLACE FUNCTION pg_catalog.read_intermediate_result(
    result_ids text[],
    format pg_catalog.citus_copy_format default 'csv')
RETURNS SETOF record
LANGUAGE C STRICT VOLATILE PARALLEL SAFE
AS 'MODULE_PATHNAME', $$read_intermediate_result_array$$;
COMMENT ON FUNCTION pg_catalog.read_intermediate_result(text[],pg_catalog.citus_copy_format)
IS 'read a set files and return them as a set of records';

