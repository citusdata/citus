/* citus--5.2-1--5.2-2.sql */

CREATE FUNCTION master_insert_query_result(distributed_table regclass, query text)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$master_insert_query_result$$;
COMMENT ON FUNCTION master_insert_query_result(regclass, text)
    IS 'append the results of a query to a distributed table'
