-- citus_columnar--13.2-1--14.0-1
-- bump version to 14.0-1

CREATE OR REPLACE VIEW columnar.storage WITH (security_barrier) AS
  SELECT c.oid::regclass AS relation,
         columnar.get_storage_id(c.oid) AS storage_id
  FROM pg_catalog.pg_class c
  JOIN pg_catalog.pg_am am ON c.relam = am.oid
  WHERE am.amname = 'columnar'
    -- exclude other sessions' temp rels, but keep *my* temp tables
    AND (c.relpersistence <> 't'
         OR c.relnamespace = pg_catalog.pg_my_temp_schema())
    AND pg_catalog.pg_has_role(c.relowner, 'USAGE');
COMMENT ON VIEW columnar.storage IS 'Columnar relation ID to storage ID mapping.';
GRANT SELECT ON columnar.storage TO PUBLIC;

-- re-emit dependent views with OR REPLACE so they stay bound cleanly
CREATE OR REPLACE VIEW columnar.stripe WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, file_offset, data_length,
         column_count, chunk_row_count, row_count, chunk_group_count, first_row_number
    FROM columnar_internal.stripe stripe, columnar.storage storage
   WHERE stripe.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.stripe
  IS 'Columnar stripe information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.stripe TO PUBLIC;

CREATE OR REPLACE VIEW columnar.chunk_group WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, chunk_group_num, row_count
    FROM columnar_internal.chunk_group cg, columnar.storage storage
   WHERE cg.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk_group
  IS 'Columnar chunk group information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk_group TO PUBLIC;

CREATE OR REPLACE VIEW columnar.chunk WITH (security_barrier) AS
  SELECT relation, storage.storage_id, stripe_num, attr_num, chunk_group_num,
         minimum_value, maximum_value, value_stream_offset, value_stream_length,
         exists_stream_offset, exists_stream_length, value_compression_type,
         value_compression_level, value_decompressed_length, value_count
    FROM columnar_internal.chunk chunk, columnar.storage storage
   WHERE chunk.storage_id = storage.storage_id;
COMMENT ON VIEW columnar.chunk
  IS 'Columnar chunk information for tables on which the current user has ownership privileges.';
GRANT SELECT ON columnar.chunk TO PUBLIC;
