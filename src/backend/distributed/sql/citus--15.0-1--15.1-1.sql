-- citus--15.0-1--15.1-1
-- bump version to 15.1-1

-- Add coordinator_uses_full_sequence_range to pg_dist_partition.
--
-- When true, the coordinator keeps the full sequence range for Citus-table
-- sequences and only workers are carved into (groupId << 48) sub-ranges.
-- When false, the coordinator is also carved into its own sub-range using
-- the coordinator's group id (0), matching how workers are handled.
--
-- Existing rows are backfilled with true to preserve prior behavior.
ALTER TABLE pg_catalog.pg_dist_partition
    ADD COLUMN coordinator_uses_full_sequence_range boolean NOT NULL DEFAULT true;

#include "udfs/citus_internal_add_partition_metadata/15.1-1.sql"
