CREATE OR REPLACE FUNCTION pg_catalog.lock_relation_if_exists(table_name text, lock_mode text, nowait boolean default False)
RETURNS BOOL
LANGUAGE C STRICT as 'MODULE_PATHNAME',
$$lock_relation_if_exists$$;
COMMENT ON FUNCTION pg_catalog.lock_relation_if_exists(table_name text, lock_mode text, nowait boolean)
IS 'locks relation in the lock_mode if the relation exists';
