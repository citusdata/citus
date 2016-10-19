/* citus--6.0-11--6.0-12.sql */

SET search_path = 'pg_catalog';

CREATE FUNCTION create_reference_table(table_name regclass)
	RETURNS void
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$create_reference_table$$;
COMMENT ON FUNCTION create_reference_table(table_name regclass)
	IS 'create a distributed reference table';

RESET search_path;
