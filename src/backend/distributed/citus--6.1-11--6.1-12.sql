/* citus--6.1-11--6.1-12.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION upgrade_to_reference_table(table_name regclass)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$upgrade_to_reference_table$$;
COMMENT ON FUNCTION upgrade_to_reference_table(table_name regclass)
    IS 'upgrades an existing broadcast table to a reference table';
    
RESET search_path;
