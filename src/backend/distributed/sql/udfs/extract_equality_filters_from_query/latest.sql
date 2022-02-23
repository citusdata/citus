CREATE OR REPLACE FUNCTION pg_catalog.extract_equality_filters_from_query(
                                      query_string text,
                                      OUT left_table_name regclass,
                                      OUT left_column_id int,
                                      OUT right_table_name regclass,
                                      OUT right_column_id int)
 RETURNS SETOF RECORD
 LANGUAGE C STRICT AS 'MODULE_PATHNAME', $$extract_equality_filters_from_query$$;
COMMENT ON FUNCTION pg_catalog.extract_equality_filters_from_query(text)
 IS 'returns equality filters and joins in the query';
