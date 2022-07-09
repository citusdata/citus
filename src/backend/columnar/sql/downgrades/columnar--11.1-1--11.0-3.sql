#include "../udfs/alter_columnar_table_set/10.0-1.sql"
#include "../udfs/alter_columnar_table_reset/10.0-1.sql"

#include "../udfs/columnar_ensure_am_depends_catalog/10.2-4.sql"

DROP VIEW columnar.options;
DROP VIEW columnar.stripe;
DROP VIEW columnar.chunk_group;
DROP VIEW columnar.chunk;
DROP VIEW columnar.storage;
DROP FUNCTION columnar.get_storage_id(regclass);

DROP SCHEMA columnar;

ALTER SCHEMA columnar_internal RENAME TO columnar;
GRANT USAGE ON SCHEMA columnar TO PUBLIC;
GRANT SELECT ON columnar.options TO PUBLIC;
GRANT SELECT ON columnar.stripe TO PUBLIC;
GRANT SELECT ON columnar.chunk_group TO PUBLIC;
