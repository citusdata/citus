-- citus--14.3-1--14.2-1
-- downgrade version to 14.2-1

-- Retain the planner-safe time_partitions definition: its columns and the
-- time_partition_range function contract are unchanged in 14.2-1.
