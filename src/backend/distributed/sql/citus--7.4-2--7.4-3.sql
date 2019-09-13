/* citus--7.4-2--7.4-3 */
SET search_path = 'pg_catalog';

-- note that we're not dropping the older version of the function
CREATE FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid, anyarray)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_hash_partition_table$$;
COMMENT ON FUNCTION worker_hash_partition_table(bigint, integer, text, text, oid,
                                                anyarray)
    IS 'hash partition query results';

RESET search_path;
