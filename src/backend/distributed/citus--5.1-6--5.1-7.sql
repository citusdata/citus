DROP FUNCTION IF EXISTS pg_catalog.worker_fetch_foreign_file(text, text, bigint, text[], integer[]);

CREATE OR REPLACE FUNCTION pg_catalog.worker_fetch_foreign_file(text, bigint, text[], integer[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_fetch_foreign_file$$;
COMMENT ON FUNCTION pg_catalog.worker_fetch_foreign_file(text, bigint, text[], integer[])
    IS 'fetch foreign file from remote node and apply file';

DROP FUNCTION IF EXISTS pg_catalog.worker_fetch_regular_table(text, text, bigint, text[], integer[]);

CREATE OR REPLACE FUNCTION pg_catalog.worker_fetch_regular_table(text, bigint, text[], integer[])
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$worker_fetch_regular_table$$;
COMMENT ON FUNCTION pg_catalog.worker_fetch_regular_table(text, bigint, text[], integer[])
    IS 'fetch PostgreSQL table from remote node';
