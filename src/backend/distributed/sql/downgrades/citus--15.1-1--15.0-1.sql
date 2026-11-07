-- citus--15.1-1--15.0-1
-- downgrade version to 15.0-1

-- Restore the 5-argument signature of citus_internal.add_partition_metadata.
DROP FUNCTION IF EXISTS citus_internal.add_partition_metadata(regclass, "char", text, integer, "char", boolean);
DROP FUNCTION IF EXISTS pg_catalog.citus_internal_add_partition_metadata(regclass, "char", text, integer, "char", boolean);
#include "../udfs/citus_internal_add_partition_metadata/13.1-1.sql"

ALTER TABLE pg_catalog.pg_dist_partition
    DROP COLUMN coordinator_uses_full_sequence_range;
