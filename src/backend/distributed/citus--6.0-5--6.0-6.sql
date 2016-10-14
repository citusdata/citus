CREATE FUNCTION worker_drop_distributed_table(logicalrelid Oid)
    RETURNS VOID
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_drop_distributed_table$$;
    
COMMENT ON FUNCTION worker_drop_distributed_table(logicalrelid Oid)
    IS 'drop the clustered table and its reference from metadata tables';
    
CREATE FUNCTION column_name_to_column(table_name regclass, column_name text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_name_to_column$$;
COMMENT ON FUNCTION column_name_to_column(table_name regclass, column_name text)
    IS 'convert a column name to its textual Var representation';
