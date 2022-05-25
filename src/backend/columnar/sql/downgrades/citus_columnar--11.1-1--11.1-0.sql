#include "../../../columnar/sql/downgrades/columnar--11.1-1--11.0-2.sql"

-- detach relations from citus_columnar

ALTER EXTENSION citus_columnar DROP SCHEMA columnar;
ALTER EXTENSION citus_columnar DROP SEQUENCE columnar.storageid_seq;
-- columnar tables
ALTER EXTENSION citus_columnar DROP TABLE columnar.options;
ALTER EXTENSION citus_columnar DROP TABLE columnar.stripe;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk_group;
ALTER EXTENSION citus_columnar DROP TABLE columnar.chunk;

ALTER EXTENSION citus_columnar DROP FUNCTION columnar.columnar_handler;
ALTER EXTENSION citus_columnar DROP ACCESS METHOD columnar;
ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_set;
ALTER EXTENSION citus_columnar DROP FUNCTION pg_catalog.alter_columnar_table_reset;

-- functions under citus_internal for columnar
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.upgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.downgrade_columnar_storage;
ALTER EXTENSION citus_columnar DROP FUNCTION citus_internal.columnar_ensure_am_depends_catalog;
