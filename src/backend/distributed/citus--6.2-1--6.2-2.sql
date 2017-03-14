/* citus--6.2-1--6.2-2.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION citus_table_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_table_size$$;
COMMENT ON FUNCTION citus_table_size(logicalrelid regclass)
    IS 'get disk space used by the specified table, excluding indexes';
    
CREATE FUNCTION citus_relation_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_relation_size$$;
COMMENT ON FUNCTION citus_relation_size(logicalrelid regclass)
    IS 'get disk space used by the ''main'' fork';
    
CREATE FUNCTION citus_total_relation_size(logicalrelid regclass)
    RETURNS bigint
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$citus_total_relation_size$$;
COMMENT ON FUNCTION citus_total_relation_size(logicalrelid regclass)
    IS 'get total disk space used by the specified table';
    
RESET search_path;
