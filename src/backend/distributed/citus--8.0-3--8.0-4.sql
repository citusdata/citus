/* citus--8.0-3--8.0-4 */
SET search_path = 'pg_catalog';

CREATE OR REPLACE FUNCTION lock_relation_if_exists(table_name text, lock_mode text)
RETURNS BOOL
LANGUAGE C STRICT as 'MODULE_PATHNAME',
$$lock_relation_if_exists$$;
COMMENT ON FUNCTION lock_relation_if_exists(table_name text, lock_mode text)
IS 'locks relation in the lock_mode if the relation exists';

RESET search_path;
