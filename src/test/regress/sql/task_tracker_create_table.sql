--
-- TASK_TRACKER_CREATE_TABLE
--

-- error

SET citus.next_shard_id TO 1070000;


-- New table definitions to test the task tracker process and protocol

CREATE TABLE lineitem_simple_task ( LIKE lineitem );
CREATE TABLE lineitem_compute_task ( LIKE lineitem );
CREATE TABLE lineitem_compute_update_task ( LIKE lineitem );

CREATE TABLE lineitem_partition_task_part_00 ( LIKE lineitem );
CREATE TABLE lineitem_partition_task_part_01 ( LIKE lineitem );
CREATE TABLE lineitem_partition_task_part_02 ( LIKE lineitem );
