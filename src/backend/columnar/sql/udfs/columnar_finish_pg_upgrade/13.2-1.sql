CREATE OR REPLACE FUNCTION pg_catalog.columnar_finish_pg_upgrade()
    RETURNS void
    LANGUAGE plpgsql
    SET search_path = pg_catalog
    AS $cppu$
BEGIN
    -- set dependencies for columnar table access method
    PERFORM columnar_internal.columnar_ensure_am_depends_catalog();
END;
$cppu$;

COMMENT ON FUNCTION pg_catalog.columnar_finish_pg_upgrade()
    IS 'perform tasks to properly complete a Postgres upgrade for columnar extension';
