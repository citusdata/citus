-- columnar--10.1-1--10.2-1.sql

-- For a proper mapping between tid & (stripe, row_num), add a new column to
-- columnar.stripe and define a BTREE index on this column.
-- Also include storage_id column for per-relation scans.
ALTER TABLE columnar.stripe ADD COLUMN first_row_number bigint;
CREATE INDEX stripe_first_row_number_idx ON columnar.stripe USING BTREE(storage_id, first_row_number);

-- Populate first_row_number column of columnar.stripe table.
--
-- For simplicity, we calculate MAX(row_count) value across all the stripes
-- of all the columanar tables and then use it to populate first_row_number
-- column. This would introduce some gaps however we are okay with that since
-- it's already the case with regular INSERT/COPY's.
DO $$
DECLARE
  max_row_count bigint;
  -- this should be equal to columnar_storage.h/COLUMNAR_FIRST_ROW_NUMBER
  COLUMNAR_FIRST_ROW_NUMBER constant bigint := 1;
BEGIN
  SELECT MAX(row_count) INTO max_row_count FROM columnar.stripe;
  UPDATE columnar.stripe SET first_row_number = COLUMNAR_FIRST_ROW_NUMBER +
                                                (stripe_num - 1) * max_row_count;
END;
$$;

#include "udfs/upgrade_columnar_storage/10.2-1.sql"
#include "udfs/downgrade_columnar_storage/10.2-1.sql"

-- upgrade storage for all columnar relations
PERFORM citus_internal.upgrade_columnar_storage(c.oid) FROM pg_class c, pg_am a
  WHERE c.relam = a.oid AND amname = 'columnar';
