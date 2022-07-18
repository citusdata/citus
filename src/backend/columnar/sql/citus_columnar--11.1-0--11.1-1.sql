-- add columnar objects back
ALTER EXTENSION citus_columnar ADD SCHEMA columnar;
ALTER EXTENSION citus_columnar ADD SCHEMA columnar_internal;
ALTER EXTENSION citus_columnar ADD SEQUENCE columnar_internal.storageid_seq;
ALTER EXTENSION citus_columnar ADD TABLE columnar_internal.options;
ALTER EXTENSION citus_columnar ADD TABLE columnar_internal.stripe;
ALTER EXTENSION citus_columnar ADD TABLE columnar_internal.chunk_group;
ALTER EXTENSION citus_columnar ADD TABLE columnar_internal.chunk;

ALTER EXTENSION citus_columnar ADD FUNCTION columnar_internal.columnar_handler;
ALTER EXTENSION citus_columnar ADD ACCESS METHOD columnar;
ALTER EXTENSION citus_columnar ADD FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus_columnar ADD FUNCTION pg_catalog.alter_columnar_table_reset;

ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus_columnar ADD FUNCTION citus_internal.columnar_ensure_am_depends_catalog;

ALTER EXTENSION citus_columnar ADD FUNCTION columnar.get_storage_id;
ALTER EXTENSION citus_columnar ADD VIEW columnar.storage;
ALTER EXTENSION citus_columnar ADD VIEW columnar.options;
ALTER EXTENSION citus_columnar ADD VIEW columnar.stripe;
ALTER EXTENSION citus_columnar ADD VIEW columnar.chunk_group;
ALTER EXTENSION citus_columnar ADD VIEW columnar.chunk;

-- move citus_internal functions to columnar_internal

ALTER FUNCTION citus_internal.upgrade_columnar_storage(regclass) SET SCHEMA columnar_internal;
ALTER FUNCTION citus_internal.downgrade_columnar_storage(regclass) SET SCHEMA columnar_internal;
ALTER FUNCTION citus_internal.columnar_ensure_am_depends_catalog() SET SCHEMA columnar_internal;


