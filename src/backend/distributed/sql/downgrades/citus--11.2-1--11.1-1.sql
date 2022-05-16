-- add relations to citus
ALTER EXTENSION citus ADD SCHEMA columnar;
ALTER EXTENSION citus ADD SEQUENCE columnar.storageid_seq;
ALTER EXTENSION citus ADD TABLE columnar.options;
ALTER EXTENSION citus ADD TABLE columnar.stripe;
ALTER EXTENSION citus ADD TABLE columnar.chunk_group;
ALTER EXTENSION citus ADD TABLE columnar.chunk;

DO $proc$
BEGIN
-- columnar functions
IF substring(current_Setting('server_version'), '\d+')::int >= 12 THEN
    EXECUTE $$
        ALTER EXTENSION citus ADD FUNCTION columnar.columnar_handler;
        ALTER EXTENSION citus ADD ACCESS METHOD columnar;
        ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_set;
        ALTER EXTENSION citus ADD FUNCTION pg_catalog.alter_columnar_table_reset;
    $$;
END IF;
END$proc$;

ALTER EXTENSION citus ADD FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus ADD FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
