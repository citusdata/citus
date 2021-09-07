CREATE FUNCTION pg_catalog.worker_nextval(sequence regclass)
    RETURNS int
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_nextval$$;
COMMENT ON FUNCTION pg_catalog.worker_nextval(regclass)
    IS 'calculates nextval() for column defaults of type int or smallint';
