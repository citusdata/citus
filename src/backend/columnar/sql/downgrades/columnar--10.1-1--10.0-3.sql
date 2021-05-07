/* columnar--10.1-1--10.0-3.sql */

-- define foreign keys between columnar metadata tables
ALTER TABLE columnar.chunk
ADD FOREIGN KEY (storage_id, stripe_num, chunk_group_num)
REFERENCES columnar.chunk_group(storage_id, stripe_num, chunk_group_num) ON DELETE CASCADE;

ALTER TABLE columnar.chunk_group
ADD FOREIGN KEY (storage_id, stripe_num)
REFERENCES columnar.stripe(storage_id, stripe_num) ON DELETE CASCADE;

-- define columnar_ensure_objects_exist again
#include "../udfs/columnar_ensure_objects_exist/10.0-1.sql"

-- downgrade storage for all columnar relations
SELECT citus_internal.downgrade_columnar_storage(c.oid) FROM pg_class c, pg_am a
  WHERE c.relam = a.oid AND amname = 'columnar';

DROP FUNCTION citus_internal.upgrade_columnar_storage(regclass);
DROP FUNCTION citus_internal.downgrade_columnar_storage(regclass);

-- drop "first_row_number" column and the index defined on it
DROP INDEX columnar.stripe_first_row_number_idx;
ALTER TABLE columnar.stripe DROP COLUMN first_row_number;
