/* citus--6.1-3--6.1-4.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION column_to_column_name(table_name regclass, column_var_text text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_to_column_name$$;
COMMENT ON FUNCTION column_to_column_name(table_name regclass, column_var_text text)
    IS 'convert the textual Var representation to a column name';

RESET search_path;
