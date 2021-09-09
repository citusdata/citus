-- columnar--10.1-1--10.0-3.sql

-- define foreign keys between columnar metadata tables
ALTER TABLE columnar.chunk
ADD FOREIGN KEY (storage_id, stripe_num, chunk_group_num)
REFERENCES columnar.chunk_group(storage_id, stripe_num, chunk_group_num) ON DELETE CASCADE;

ALTER TABLE columnar.chunk_group
ADD FOREIGN KEY (storage_id, stripe_num)
REFERENCES columnar.stripe(storage_id, stripe_num) ON DELETE CASCADE;

-- define columnar_ensure_objects_exist again
#include "../udfs/columnar_ensure_objects_exist/10.0-1.sql"
