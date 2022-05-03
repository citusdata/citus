#include "udfs/alter_columnar_table_set/11.1-1.sql"
#include "udfs/alter_columnar_table_reset/11.1-1.sql"

-- rename columnar schema to columnar_internal and tighten security

REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA columnar FROM PUBLIC;
ALTER SCHEMA columnar RENAME TO columnar_internal;
REVOKE ALL PRIVILEGES ON SCHEMA columnar_internal FROM PUBLIC;

-- create columnar schema with public usage privileges

CREATE SCHEMA columnar;
GRANT USAGE ON SCHEMA columnar TO PUBLIC;

-- update UDF to account for columnar_internal schema
#include "udfs/columnar_ensure_am_depends_catalog/11.1-1.sql"

-- add utility function

CREATE FUNCTION columnar.get_storage_id(regclass) RETURNS bigint
    LANGUAGE C STRICT
    AS 'citus_columnar', $$columnar_relation_storageid$$;

-- create views for columnar table information

CREATE VIEW columnar.storage WITH (security_barrier) AS
  SELECT c.oid::regclass AS relation,
         columnar.get_storage_id(c.oid) AS storage_id
    FROM pg_class c, pg_am am
    WHERE c.relam = am.oid AND am.amname = 'columnar'
      AND pg_has_role(c.relowner, 'USAGE');
COMMENT ON VIEW columnar.storage IS 'Columnar relation ID to storage ID mapping.';
GRANT SELECT ON columnar.storage TO PUBLIC;

CREATE VIEW columnar.options WITH (security_barrier) AS
  SELECT regclass AS relation, chunk_group_row_limit,
         stripe_row_limit, compression, compression_level
    FROM columnar_internal.options o, pg_class c
    WHERE o.regclass = c.oid
      AND pg_has_role(c.relowner, 'USAGE');
COMMENT ON VIEW columnar.options
  IS 'Columnar options for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.options TO PUBLIC;

CREATE VIEW columnar.stripe WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, file_offset, data_length,
         column_count, chunk_row_count, row_count, chunk_group_count, first_row_number
    FROM columnar_internal.stripe stripe, columnar.storage storage
    WHERE stripe.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.stripe
  IS 'Columnar stripe information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.stripe TO PUBLIC;

CREATE VIEW columnar.chunk_group WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, chunk_group_num, row_count
    FROM columnar_internal.chunk_group cg, columnar.storage storage
    WHERE cg.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk_group
  IS 'Columnar chunk group information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk_group TO PUBLIC;

CREATE VIEW columnar.chunk WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, attr_num, chunk_group_num,
         minimum_value, maximum_value, value_stream_offset, value_stream_length,
         exists_stream_offset, exists_stream_length, value_compression_type,
         value_compression_level, value_decompressed_length, value_count
    FROM columnar_internal.chunk chunk, columnar.storage storage
    WHERE chunk.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk
  IS 'Columnar chunk information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk TO PUBLIC;
