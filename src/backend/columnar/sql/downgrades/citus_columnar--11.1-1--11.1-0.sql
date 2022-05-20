-- detach relations from citus_columnar

ALTER EXTENSION citus_columnar DROP SCHEMA columnar;
ALTER EXTENSION citus_columnar DROP SEQUENCE columnar.storageid_seq;
-- columnar tables
ALTER EXTENSION citus_columnar DROP TABLE columnar.options;
ALTER EXTENSION citus_columnar DROP TABLE columnar.stripe;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk_group;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk;

DO $proc$
BEGIN
-- columnar functions
IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
    EXECUTE $$
        ALTER EXTENSION citus_columnar DROP FUNCTION columnar.columnar_handler;
        ALTER EXTENSION citus_columnar DROP ACCESS METHOD columnar;
        ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_set;
        ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_reset;
    $$;
END IF;
END$proc$;

-- functions under citus_internal for columnar
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
